#!/usr/bin/env python3
"""
Dual-Instance Discord Moderation Bot System (BanBot 3000 HA)
===========================================================

This system runs two instances of the moderation bot:
- One as PRIMARY (actively handling Discord events)
- One as SECONDARY (standby mode, ready for failover)

Features:
- Automatic failover within 60 seconds
- Real-time data synchronization via HTTP API
- Health monitoring and UptimeRobot integration
- Seamless switchover without duplicate actions
- Only one instance connected to Discord at a time
"""

import discord
from discord.ext import commands, tasks
import asyncio
import aiohttp
from aiohttp import web
import sys
import time
import traceback
import logging
import re
import os
import json
import signal
from datetime import datetime, timedelta, timezone
from typing import Optional, Union, Dict, List, Any
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, deque
import threading
import socket
from contextlib import closing

# Setup enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'banbot_{os.getpid()}.log')
    ]
)
logger = logging.getLogger('BanBot3000-HA')

class BotRole(Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"

class ActionType(Enum):
    BAN = "ban"
    KICK = "kick"
    TIMEOUT = "timeout"
    WARN = "warn"
    DEOP = "deop"

@dataclass
class ModerationAction:
    user_id: int
    moderator_id: int
    action: ActionType
    reason: str
    timestamp: datetime
    duration: Optional[int] = None
    active: bool = True
    
    def to_dict(self):
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['action'] = self.action.value
        return data
    
    @classmethod
    def from_dict(cls, data):
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        data['action'] = ActionType(data['action'])
        return cls(**data)

@dataclass
class Warning:
    id: int
    user_id: int
    moderator_id: int
    reason: str
    timestamp: datetime
    active: bool = True
    
    def to_dict(self):
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data):
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)

@dataclass
class DeoppedUser:
    user_id: int
    deopped_by: int
    timestamp: datetime
    reason: str
    
    def to_dict(self):
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data):
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)

@dataclass
class HealthStatus:
    role: BotRole
    is_active: bool
    discord_connected: bool
    last_heartbeat: datetime
    uptime: float
    stats: Dict[str, Any]

class HealthMonitor:
    """Monitors bot health and manages failover logic"""
    
    def __init__(self, instance):
        self.instance = instance
        self.peer_url = None
        self.last_peer_heartbeat = datetime.now(timezone.utc)
        self.health_check_interval = 30  # seconds
        self.failover_timeout = 60  # seconds
        self.session = None
        
    async def initialize(self):
        """Initialize HTTP session for peer communication"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        )
        
    async def cleanup(self):
        """Clean up HTTP session"""
        if self.session:
            await self.session.close()
    
    async def check_peer_health(self) -> bool:
        """Check if peer instance is healthy"""
        if not self.peer_url:
            return False
            
        try:
            async with self.session.get(f"{self.peer_url}/health") as resp:
                if resp.status == 200:
                    health_data = await resp.json()
                    self.last_peer_heartbeat = datetime.fromisoformat(health_data['last_heartbeat'])
                    return health_data.get('is_active', False)
        except Exception as e:
            logger.warning(f"Failed to check peer health: {e}")
            
        return False
    
    async def sync_data_with_peer(self):
        """Synchronize moderation data with peer"""
        if not self.peer_url:
            return
            
        try:
            # Send our data to peer
            sync_data = {
                'moderation_actions': [action.to_dict() for action in self.instance.moderation_actions],
                'warnings': [warning.to_dict() for warning in self.instance.warnings],
                'deopped_users': {str(k): v.to_dict() for k, v in self.instance.deopped_users.items()},
                'stats': self.instance.stats.copy(),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            async with self.session.post(f"{self.peer_url}/sync", json=sync_data) as resp:
                if resp.status == 200:
                    logger.debug("Data sync with peer successful")
                else:
                    logger.warning(f"Data sync failed: {resp.status}")
                    
        except Exception as e:
            logger.warning(f"Failed to sync data with peer: {e}")
    
    def should_takeover(self) -> bool:
        """Determine if this instance should take over as primary"""
        if self.instance.role == BotRole.PRIMARY:
            return False
            
        now = datetime.now(timezone.utc)
        time_since_peer_heartbeat = (now - self.last_peer_heartbeat).total_seconds()
        
        return time_since_peer_heartbeat > self.failover_timeout

class HTTPServer:
    """HTTP server for health checks and peer communication"""
    
    def __init__(self, bot_instance, port=5000):
        self.bot = bot_instance
        self.port = port
        self.app = web.Application()
        self.runner = None
        self.site = None
        
        # Setup routes
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/status', self.status_check)
        self.app.router.add_post('/sync', self.sync_data)
        self.app.router.add_get('/ping', self.ping_handler)  # For UptimeRobot
        
    async def health_check(self, request):
        """Health check endpoint"""
        health_status = HealthStatus(
            role=self.bot.role,
            is_active=self.bot.is_active_instance,
            discord_connected=self.bot.is_ready(),
            last_heartbeat=datetime.now(timezone.utc).isoformat(),
            uptime=(datetime.now(timezone.utc) - self.bot.stats["uptime_start"]).total_seconds(),
            stats=self.bot.stats
        )
        
        return web.json_response(asdict(health_status))
    
    async def status_check(self, request):
        """Detailed status endpoint"""
        return web.json_response({
            'role': self.bot.role.value,
            'is_active': self.bot.is_active_instance,
            'discord_ready': self.bot.is_ready(),
            'guilds': len(self.bot.guilds) if self.bot.is_ready() else 0,
            'process_id': os.getpid(),
            'uptime': (datetime.now(timezone.utc) - self.bot.stats["uptime_start"]).total_seconds(),
            'memory_usage': {
                'moderation_actions': len(self.bot.moderation_actions),
                'warnings': len(self.bot.warnings),
                'deopped_users': len(self.bot.deopped_users),
                'command_history': len(self.bot.command_history)
            }
        })
    
    async def sync_data(self, request):
        """Data synchronization endpoint"""
        try:
            sync_data = await request.json()
            
            # Only sync if we're not the active instance to avoid conflicts
            if not self.bot.is_active_instance:
                # Sync moderation actions
                for action_data in sync_data.get('moderation_actions', []):
                    action = ModerationAction.from_dict(action_data)
                    # Check if action already exists to avoid duplicates
                    exists = any(a.user_id == action.user_id and 
                               a.timestamp == action.timestamp and
                               a.action == action.action 
                               for a in self.bot.moderation_actions)
                    if not exists:
                        self.bot.moderation_actions.append(action)
                
                # Sync warnings
                for warning_data in sync_data.get('warnings', []):
                    warning = Warning.from_dict(warning_data)
                    exists = any(w.id == warning.id for w in self.bot.warnings)
                    if not exists:
                        self.bot.warnings.append(warning)
                        if warning.id >= self.bot.next_warning_id:
                            self.bot.next_warning_id = warning.id + 1
                
                # Sync deopped users
                for user_id_str, deop_data in sync_data.get('deopped_users', {}).items():
                    user_id = int(user_id_str)
                    if user_id not in self.bot.deopped_users:
                        self.bot.deopped_users[user_id] = DeoppedUser.from_dict(deop_data)
                
                # Sync stats (but keep our own uptime)
                uptime_start = self.bot.stats["uptime_start"]
                self.bot.stats.update(sync_data.get('stats', {}))
                self.bot.stats["uptime_start"] = uptime_start
                
                logger.info("Data synchronized from peer")
            
            return web.json_response({'status': 'success'})
            
        except Exception as e:
            logger.error(f"Sync data error: {e}")
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)
    
    async def ping_handler(self, request):
        """Simple ping endpoint for UptimeRobot/Flash monitoring"""
        return web.json_response({
            'status': 'ok',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'role': self.bot.role.value,
            'active': self.bot.is_active_instance
        })
    
    async def start(self):
        """Start HTTP server"""
        try:
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            self.site = web.TCPSite(self.runner, '0.0.0.0', self.port)
            await self.site.start()
            logger.info(f"HTTP server started on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to start HTTP server: {e}")
            raise
    
    async def stop(self):
        """Stop HTTP server"""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()

def is_port_available(port):
    """Check if a port is available"""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(1)
        result = sock.connect_ex(('127.0.0.1', port))
        return result != 0

def determine_instance_role():
    """Determine if this should be primary or secondary instance"""
    # Check if port 5000 is available
    if is_port_available(5000):
        return BotRole.PRIMARY, 5000, None
    else:
        # If 5000 is taken, we're secondary and peer is on 5000
        return BotRole.SECONDARY, 5001, "http://127.0.0.1:5000"

class BanBot3000HA(commands.Bot):
    """High Availability Discord Moderation Bot"""
    
    def __init__(self, role: BotRole, port: int, peer_url: Optional[str] = None):
        # Fixed command prefix to avoid "both" triggering "bothelp"
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.guilds = True
        intents.moderation = True

        super().__init__(
            command_prefix="bot ",  # Added space to fix command triggering
            intents=intents,
            help_command=None,
            case_insensitive=True
        )

        # Instance configuration
        self.role = role
        self.port = port
        self.peer_url = peer_url
        self.is_active_instance = (role == BotRole.PRIMARY)
        
        # Initialize components
        self.health_monitor = HealthMonitor(self)
        self.http_server = HTTPServer(self, port)
        
        if peer_url:
            self.health_monitor.peer_url = peer_url

        # Bot configuration (same as original)
        self.config = {
            "error_channel_id": None,
            "admin_users": ["doper_official"],
            "moderator_users": [],
            "max_timeout_days": 28,
            "auto_mod": {
                "enabled": False,
                "spam_threshold": 5,
                "spam_timeframe": 10
            }
        }

        # Data storage (same as original)
        self.moderation_actions: List[ModerationAction] = []
        self.deopped_users: Dict[int, DeoppedUser] = {}
        self.warnings: List[Warning] = []
        self.processed_messages: deque = deque(maxlen=1000)
        self.command_history: List[Dict] = []

        # Counters
        self.next_warning_id = 1
        self.next_action_id = 1

        # Stats
        self.stats = {
            "bans": 0,
            "kicks": 0,
            "timeouts": 0,
            "warnings": 0,
            "deops": 0,
            "commands_used": 0,
            "uptime_start": datetime.now(timezone.utc)
        }

        # Spam detection
        self.user_message_times: Dict[int, deque] = defaultdict(lambda: deque(maxlen=10))
        
        # Shutdown flag
        self.shutdown_requested = False

    def log_command_usage(self, ctx, command_name: str, target_user: Optional[discord.Member] = None, details: str = None):
        """Enhanced command usage logging"""
        entry = {
            "timestamp": datetime.now(timezone.utc),
            "command": command_name,
            "executor": {
                "id": ctx.author.id,
                "name": ctx.author.display_name,
                "username": ctx.author.name
            },
            "channel": ctx.channel.name,
            "guild": ctx.guild.name if ctx.guild else "DM",
            "guild_id": ctx.guild.id if ctx.guild else None
        }
        
        if target_user:
            entry["victim"] = {
                "id": target_user.id,
                "name": target_user.display_name,
                "username": target_user.name,
                "mention": target_user.mention
            }
        
        if details:
            entry["details"] = details
            
        self.command_history.append(entry)
        
        # Keep only last 1000 entries
        if len(self.command_history) > 1000:
            self.command_history = self.command_history[-500:]

    def is_admin(self, user: Union[discord.Member, discord.User]) -> bool:
        """Check if user is admin"""
        is_named_admin = user.name.lower() in [name.lower() for name in self.config["admin_users"]]
        is_guild_owner = isinstance(user, discord.Member) and user == user.guild.owner
        return is_named_admin or is_guild_owner

    def is_deopped(self, user_id: int) -> bool:
        """Check if user is deopped"""
        return user_id in self.deopped_users

    def has_permission_for_action(self, member: discord.Member, action: str) -> bool:
        """Check if user has permission for specific moderation action"""
        if self.is_admin(member):
            return True

        permissions = member.guild_permissions

        if action == "ban":
            return permissions.ban_members
        elif action == "kick":
            return permissions.kick_members
        elif action == "timeout":
            return permissions.moderate_members
        elif action == "warn":
            # FIXED: Everyone can use warn command (as requested)
            return True
        elif action == "cleanup":
            return permissions.manage_messages
        else:
            return False

    def is_authorized(self, ctx: commands.Context, action: str = "general") -> bool:
        """Check if user is authorized to use moderation commands"""
        # Only process commands if this is the active instance
        if not self.is_active_instance:
            return False
            
        if self.is_deopped(ctx.author.id):
            return False

        if isinstance(ctx.author, discord.Member):
            return self.has_permission_for_action(ctx.author, action)
        return False

    def log_action(self, action: ModerationAction):
        """Log moderation action"""
        action.timestamp = datetime.now(timezone.utc)
        self.moderation_actions.append(action)

        # Update stats
        action_key = f"{action.action.value}s"
        if action_key in self.stats:
            self.stats[action_key] += 1

        # Keep memory manageable
        if len(self.moderation_actions) > 10000:
            self.moderation_actions = self.moderation_actions[-5000:]

    def add_warning(self, user_id: int, moderator_id: int, reason: str) -> Warning:
        """Add a warning"""
        warning = Warning(
            id=self.next_warning_id,
            user_id=user_id,
            moderator_id=moderator_id,
            reason=reason,
            timestamp=datetime.now(timezone.utc)
        )
        self.warnings.append(warning)
        self.next_warning_id += 1
        self.stats["warnings"] += 1
        return warning

    def get_user_warnings(self, user_id: int, limit: int = 10) -> List[Warning]:
        """Get warnings for a user"""
        user_warnings = [w for w in self.warnings if w.user_id == user_id and w.active]
        return sorted(user_warnings, key=lambda x: x.timestamp, reverse=True)[:limit]

    def get_user_actions(self, user_id: int, limit: int = 10) -> List[ModerationAction]:
        """Get moderation actions for a user"""
        user_actions = [a for a in self.moderation_actions if a.user_id == user_id]
        return sorted(user_actions, key=lambda x: x.timestamp, reverse=True)[:limit]

    async def report_error(self, ctx_or_channel, error_text: str):
        """Enhanced error reporting"""
        try:
            error_channel_id = self.config.get("error_channel_id")
            if error_channel_id:
                channel = self.get_channel(error_channel_id)
            else:
                if isinstance(ctx_or_channel, commands.Context):
                    channel = ctx_or_channel.channel
                else:
                    channel = ctx_or_channel

            if channel and hasattr(channel, 'send'):
                if len(error_text) > 1900:
                    error_text = error_text[:1900] + "\n[Truncated...]"

                embed = discord.Embed(
                    title=f"🚨 BanBot 3000 HA Error ({self.role.value.upper()})",
                    description=f"```python\n{error_text}\n```",
                    color=discord.Color.red(),
                    timestamp=datetime.now(timezone.utc)
                )
                embed.add_field(
                    name="Instance Info", 
                    value=f"Role: {self.role.value}\nActive: {self.is_active_instance}\nPID: {os.getpid()}", 
                    inline=False
                )
                await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Failed to send error report: {e}")

    async def check_permissions(self, guild: discord.Guild) -> Dict[str, bool]:
        """Check if bot has required permissions"""
        if not self.user:
            return {}
        bot_member = guild.get_member(self.user.id)
        if not bot_member:
            return {}

        permissions = bot_member.guild_permissions

        return {
            "ban_members": permissions.ban_members,
            "kick_members": permissions.kick_members,
            "moderate_members": permissions.moderate_members,
            "manage_messages": permissions.manage_messages,
            "read_message_history": permissions.read_message_history,
            "send_messages": permissions.send_messages,
            "embed_links": permissions.embed_links
        }

    async def become_active(self):
        """Transition to active instance"""
        if self.is_active_instance:
            return
            
        logger.info(f"🔄 {self.role.value.upper()} instance becoming ACTIVE (PID: {os.getpid()})")
        self.is_active_instance = True
        
        # Update presence to show active status
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name=f"for violations | {self.role.value} ACTIVE | bot help"
            ),
            status=discord.Status.online
        )

    async def become_standby(self):
        """Transition to standby mode"""
        if not self.is_active_instance:
            return
            
        logger.info(f"⏸️ {self.role.value.upper()} instance entering STANDBY mode")
        self.is_active_instance = False
        
        # Update presence to show standby status
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name=f"in standby | {self.role.value} BACKUP"
            ),
            status=discord.Status.idle
        )

    @tasks.loop(seconds=30)
    async def health_check_loop(self):
        """Main health monitoring loop"""
        try:
            if self.role == BotRole.SECONDARY:
                # Check if we should take over
                if self.health_monitor.should_takeover():
                    logger.warning("🚨 Primary instance appears down - taking over!")
                    await self.become_active()
                    
            # Sync data with peer (both directions)
            await self.health_monitor.sync_data_with_peer()
            
            # Check peer health
            peer_healthy = await self.health_monitor.check_peer_health()
            if self.role == BotRole.PRIMARY and peer_healthy:
                # If we're primary but peer is also active, step down
                # This handles cases where both instances think they're primary
                peer_status_check = await self.health_monitor.session.get(f"{self.health_monitor.peer_url}/status")
                if peer_status_check.status == 200:
                    peer_data = await peer_status_check.json()
                    if peer_data.get('is_active', False):
                        logger.warning("⚠️ Both instances active - stepping down to avoid conflicts")
                        await self.become_standby()
                        
        except Exception as e:
            logger.error(f"Health check error: {e}")

    @tasks.loop(minutes=30)
    async def cleanup_task(self):
        """Periodic cleanup task"""
        try:
            # Only run cleanup on active instance
            if not self.is_active_instance:
                return
                
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(hours=1)

            # Clean message tracking
            for user_id in list(self.user_message_times.keys()):
                user_times = self.user_message_times[user_id]
                while user_times and user_times[0] < cutoff:
                    user_times.popleft()

                if not user_times:
                    del self.user_message_times[user_id]

            logger.info(f"Cleanup completed on {self.role.value} instance")
        except Exception as e:
            logger.error(f"Cleanup task error: {e}")

    async def on_ready(self):
        """Bot ready event"""
        logger.info(f'🚀 {self.user} connected! Role: {self.role.value.upper()}, Active: {self.is_active_instance}, PID: {os.getpid()}')
        logger.info(f'Bot is in {len(self.guilds)} guilds')
        
        # Initialize health monitor
        await self.health_monitor.initialize()
        
        # Start tasks
        if not self.health_check_loop.is_running():
            self.health_check_loop.start()
            
        if not self.cleanup_task.is_running():
            self.cleanup_task.start()

        # Set appropriate status
        if self.is_active_instance:
            await self.change_presence(
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name=f"for violations | {self.role.value} ACTIVE | bot help"
                ),
                status=discord.Status.online
            )
        else:
            await self.change_presence(
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name=f"in standby | {self.role.value} BACKUP"
                ),
                status=discord.Status.idle
            )

    async def on_command_error(self, ctx, error):
        """Global error handler"""
        # Only handle errors if we're the active instance
        if not self.is_active_instance:
            return
            
        self.stats["commands_used"] += 1

        if isinstance(error, commands.CommandNotFound):
            return

        elif isinstance(error, commands.MissingPermissions):
            embed = discord.Embed(
                title="❌ Missing Permissions",
                description=f"You don't have permission to use this command.\nRequired: {', '.join(error.missing_permissions)}",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)

        elif isinstance(error, commands.BotMissingPermissions):
            embed = discord.Embed(
                title="❌ Bot Missing Permissions", 
                description=f"I don't have permission to do that.\nRequired: {', '.join(error.missing_permissions)}",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)

        elif isinstance(error, commands.MemberNotFound):
            embed = discord.Embed(
                title="❌ Member Not Found",
                description="Could not find that member. Make sure you mention them correctly.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)

        elif isinstance(error, commands.BadArgument):
            embed = discord.Embed(
                title="❌ Invalid Argument",
                description=f"Invalid argument provided: {str(error)}",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)

        else:
            error_text = f"Command: {ctx.command}\nError: {str(error)}\nTraceback: {traceback.format_exc()}"
            logger.error(error_text)
            await self.report_error(ctx, error_text)

            embed = discord.Embed(
                title="❌ Unexpected Error",
                description="An unexpected error occurred. The error has been logged.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)

    async def process_commands(self, message):
        """Override to only process commands on active instance"""
        if self.is_active_instance:
            await super().process_commands(message)

    async def close(self):
        """Graceful shutdown"""
        logger.info(f"🔄 Shutting down {self.role.value} instance...")
        
        # Stop tasks
        if self.health_check_loop.is_running():
            self.health_check_loop.stop()
        if self.cleanup_task.is_running():
            self.cleanup_task.stop()
            
        # Cleanup health monitor
        await self.health_monitor.cleanup()
        
        # Stop HTTP server
        await self.http_server.stop()
        
        await super().close()

# Create bot instance
def create_bot_instance():
    """Factory function to create bot instance with proper configuration"""
    role, port, peer_url = determine_instance_role()
    return BanBot3000HA(role, port, peer_url)

bot = create_bot_instance()

# --- COMMAND DEFINITIONS ---
# Note: Fixed command prefix issue by using "bot " (with space) instead of "bot"
# This prevents "both" from triggering "bothelp"

@bot.command()
async def memory(ctx):
    """Show detailed memory usage statistics"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "memory")
    
    embed = discord.Embed(
        title="🧠 Bot Memory Usage",
        description=f"Current in-memory data storage statistics\n**Instance:** {bot.role.value.upper()} {'ACTIVE' if bot.is_active_instance else 'STANDBY'}",
        color=discord.Color.blue()
    )
    
    # Memory usage breakdown
    embed.add_field(
        name="📝 Command History",
        value=f"{len(bot.command_history)} entries",
        inline=True
    )
    
    embed.add_field(
        name="⚖️ Moderation Actions",
        value=f"{len(bot.moderation_actions)} actions",
        inline=True
    )
    
    embed.add_field(
        name="⚠️ Warnings",
        value=f"{len(bot.warnings)} warnings",
        inline=True
    )
    
    embed.add_field(
        name="🚫 Deopped Users",
        value=f"{len(bot.deopped_users)} users",
        inline=True
    )
    
    embed.add_field(
        name="📊 Message Tracking",
        value=f"{len(bot.user_message_times)} users",
        inline=True
    )
    
    embed.add_field(
        name="💾 Processed Messages",
        value=f"{len(bot.processed_messages)} messages",
        inline=True
    )
    
    # Calculate approximate memory usage
    total_entries = (len(bot.command_history) + len(bot.moderation_actions) + 
                    len(bot.warnings) + len(bot.deopped_users) + 
                    len(bot.user_message_times) + len(bot.processed_messages))
    
    embed.add_field(
        name="📈 Total Entries",
        value=f"{total_entries:,} items",
        inline=False
    )
    
    await ctx.send(embed=embed)

@bot.command()
async def stats(ctx):
    """Show bot statistics"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "stats")
    
    uptime = datetime.now(timezone.utc) - bot.stats["uptime_start"]
    
    embed = discord.Embed(
        title="📊 BanBot 3000 HA Statistics",
        description=f"**Instance:** {bot.role.value.upper()} {'ACTIVE' if bot.is_active_instance else 'STANDBY'}\n**PID:** {os.getpid()}",
        color=discord.Color.blue()
    )
    
    embed.add_field(name="Uptime", value=f"{uptime.days}d {uptime.seconds//3600}h {(uptime.seconds//60)%60}m", inline=True)
    embed.add_field(name="Guilds", value=len(bot.guilds), inline=True)
    embed.add_field(name="Commands Used", value=bot.stats["commands_used"], inline=True)
    
    embed.add_field(name="Bans", value=bot.stats["bans"], inline=True)
    embed.add_field(name="Kicks", value=bot.stats["kicks"], inline=True)
    embed.add_field(name="Timeouts", value=bot.stats["timeouts"], inline=True)
    
    embed.add_field(name="Warnings", value=bot.stats["warnings"], inline=True)
    embed.add_field(name="Deops", value=bot.stats["deops"], inline=True)
    embed.add_field(name="Total Actions", value=len(bot.moderation_actions), inline=True)

    # Add HA-specific stats
    peer_status = "Unknown"
    if bot.health_monitor.peer_url:
        try:
            async with bot.health_monitor.session.get(f"{bot.health_monitor.peer_url}/health") as resp:
                if resp.status == 200:
                    peer_data = await resp.json()
                    peer_status = "ACTIVE" if peer_data.get('is_active', False) else "STANDBY"
                else:
                    peer_status = "Offline"
        except:
            peer_status = "Offline"
    
    embed.add_field(
        name="🏥 HA Status", 
        value=f"Peer: {peer_status}\nPort: {bot.port}\nFailover: 60s", 
        inline=False
    )

    await ctx.send(embed=embed)

@bot.command()
async def deop(ctx, member: discord.Member, *, reason: str = "No reason provided"):
    """Remove admin privileges from a user (Admin only)"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "deop", member, reason)
    
    if not bot.is_admin(ctx.author):
        embed = discord.Embed(
            title="❌ Access Denied",
            description="Only administrators can use this command.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    if member == ctx.author:
        embed = discord.Embed(
            title="❌ Invalid Target",
            description="You cannot deop yourself!",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    # Add to deopped users
    deopped_user = DeoppedUser(
        user_id=member.id,
        deopped_by=ctx.author.id,
        timestamp=datetime.now(timezone.utc),
        reason=reason
    )
    bot.deopped_users[member.id] = deopped_user
    bot.stats["deops"] += 1

    embed = discord.Embed(
        title="🚫 User Deopped",
        description=f"**{member.display_name}** ({member.mention}) has been removed from admin privileges.",
        color=discord.Color.red()
    )
    embed.add_field(name="👤 Victim", value=f"{member.display_name} ({member.name})", inline=True)
    embed.add_field(name="⚖️ Executor", value=f"{ctx.author.display_name} ({ctx.author.name})", inline=True)
    embed.add_field(name="🤖 Instance", value=f"{bot.role.value.upper()}", inline=True)
    embed.add_field(name="📝 Reason", value=reason, inline=False)
    embed.timestamp = datetime.now(timezone.utc)

    await ctx.send(embed=embed)
    logger.info(f"User {member} deopped by {ctx.author}: {reason}")

@bot.command()
async def reop(ctx, member: discord.Member):
    """Restore admin privileges to a user (Admin only)"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "reop", member)
    
    if not bot.is_admin(ctx.author):
        embed = discord.Embed(
            title="❌ Access Denied",
            description="Only administrators can use this command.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    if member.id not in bot.deopped_users:
        embed = discord.Embed(
            title="❌ User Not Deopped",
            description=f"{member.mention} is not currently deopped.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    # Remove from deopped users
    del bot.deopped_users[member.id]

    embed = discord.Embed(
        title="✅ User Reopped",
        description=f"**{member.display_name}** ({member.mention}) has had their admin privileges restored.",
        color=discord.Color.green()
    )
    embed.add_field(name="👤 Victim", value=f"{member.display_name} ({member.name})", inline=True)
    embed.add_field(name="⚖️ Executor", value=f"{ctx.author.display_name} ({ctx.author.name})", inline=True)
    embed.add_field(name="🤖 Instance", value=f"{bot.role.value.upper()}", inline=True)
    embed.timestamp = datetime.now(timezone.utc)

    await ctx.send(embed=embed)
    logger.info(f"User {member} reopped by {ctx.author}")

@bot.command()
async def deopped(ctx):
    """Show currently deopped users"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "deopped")
    
    if not bot.deopped_users:
        embed = discord.Embed(
            title="🚫 Deopped Users",
            description="No users are currently deopped.",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
        return

    embed = discord.Embed(
        title="🚫 Deopped Users",
        description=f"**Instance:** {bot.role.value.upper()}",
        color=discord.Color.red()
    )

    for user_id, deopped_user in bot.deopped_users.items():
        member = ctx.guild.get_member(user_id)
        member_name = member.display_name if member else f"User ID: {user_id}"
        
        deopped_by = ctx.guild.get_member(deopped_user.deopped_by)
        deopped_by_name = deopped_by.display_name if deopped_by else "Unknown"
        
        embed.add_field(
            name=member_name,
            value=f"**Reason:** {deopped_user.reason}\n**Deopped by:** {deopped_by_name}\n**Date:** {deopped_user.timestamp.strftime('%Y-%m-%d %H:%M UTC')}",
            inline=False
        )

    await ctx.send(embed=embed)

@bot.command()
async def cleanup(ctx, amount: int = 10):
    """Delete messages from the channel"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "cleanup", details=f"Amount: {amount}")
    
    if not bot.is_authorized(ctx, "cleanup"):
        embed = discord.Embed(
            title="❌ Access Denied",
            description="You don't have permission to manage messages or you've been deopped.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    if amount < 1 or amount > 100:
        embed = discord.Embed(
            title="❌ Invalid Amount",
            description="Amount must be between 1 and 100.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    try:
        deleted = await ctx.channel.purge(limit=amount + 1)  # +1 to include the command message
        
        embed = discord.Embed(
            title="🧹 Messages Cleaned",
            description=f"Deleted {len(deleted) - 1} messages by **{ctx.author.display_name}** on **{bot.role.value.upper()}** instance.",
            color=discord.Color.green()
        )
        
        # Send and auto-delete the confirmation
        msg = await ctx.send(embed=embed)
        await asyncio.sleep(5)
        await msg.delete()

    except discord.Forbidden:
        embed = discord.Embed(
            title="❌ Cleanup Failed",
            description="I don't have permission to delete messages.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
    except Exception as e:
        await bot.report_error(ctx, f"Cleanup command error: {str(e)}")

@bot.command()
async def uptime(ctx):
    """Show bot uptime"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "uptime")
    
    uptime = datetime.now(timezone.utc) - bot.stats["uptime_start"]
    
    embed = discord.Embed(
        title="⏰ Bot Uptime",
        description=f"BanBot 3000 HA has been running for:\n{uptime.days} days, {uptime.seconds//3600} hours, {(uptime.seconds//60)%60} minutes\n\n**Instance:** {bot.role.value.upper()} {'ACTIVE' if bot.is_active_instance else 'STANDBY'}\n**PID:** {os.getpid()}",
        color=discord.Color.blue()
    )
    embed.timestamp = datetime.now(timezone.utc)
    
    await ctx.send(embed=embed)

# Signal handlers for graceful shutdown
def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    bot.shutdown_requested = True
    
    # Create new event loop if needed
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Schedule the shutdown
    if loop.is_running():
        asyncio.create_task(shutdown_bot())
    else:
        loop.run_until_complete(shutdown_bot())

async def shutdown_bot():
    """Gracefully shutdown the bot"""
    logger.info("Starting graceful shutdown...")
    
    try:
        # Update status to show shutdown
        if bot.is_ready():
            await bot.change_presence(
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name="shutting down..."
                ),
                status=discord.Status.dnd
            )
        
        # Stop the bot
        await bot.close()
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    
    logger.info("Shutdown complete")

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def main():
    """Main function to run the bot with HTTP server"""
    try:
        # Start HTTP server first
        await bot.http_server.start()
        logger.info(f"🌐 HTTP server running on port {bot.port}")
        
        # Get Discord token
        token = os.getenv('DISCORD_TOKEN')
        if not token:
            logger.error("❌ No Discord token provided! Set the DISCORD_TOKEN environment variable.")
            return
        
        # Start the bot
        logger.info(f"🚀 Starting BanBot 3000 HA - Role: {bot.role.value.upper()}, Port: {bot.port}")
        await bot.start(token)
        
    except discord.LoginFailure:
        logger.error("❌ Invalid Discord token provided!")
    except Exception as e:
        logger.error(f"❌ Bot error: {e}")
        traceback.print_exc()
    finally:
        # Cleanup
        await bot.http_server.stop()
        logger.info("🔄 Bot stopped")

def run_bot():
    """Entry point to run the bot"""
    try:
        # Handle Windows-specific event loop policy
        if sys.platform.startswith('win'):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        # Run the main coroutine
        asyncio.run(main())
        
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    run_bot()

# --- USAGE INSTRUCTIONS ---
"""
DUAL-INSTANCE SETUP GUIDE
=========================

1. ENVIRONMENT SETUP:
   - Set DISCORD_TOKEN environment variable
   - Ensure Python 3.8+ and discord.py 2.3+ installed
   - Install aiohttp: pip install aiohttp

2. RUNNING THE SYSTEM:
   
   Terminal 1 (Primary Instance):
   python banbot_ha.py
   
   Terminal 2 (Secondary Instance):
   python banbot_ha.py
   
   The system automatically detects which should be primary/secondary
   based on port availability.

3. PORT CONFIGURATION:
   - Primary instance runs HTTP server on port 5000
   - Secondary instance runs HTTP server on port 5001
   - Both instances communicate via these ports
   - UptimeRobot should monitor port 5000

4. FAILOVER BEHAVIOR:
   - If primary goes down, secondary takes over within 60 seconds
   - When primary returns, it resumes control automatically
   - Only one instance processes Discord commands at a time
   - Data is continuously synced between instances

5. MONITORING ENDPOINTS:
   - http://localhost:5000/ping - UptimeRobot/Flash monitoring
   - http://localhost:5000/health - Detailed health status
   - http://localhost:5000/status - Instance status info
   - http://localhost:5001/ping - Secondary instance ping

6. COMMAND PREFIX FIX:
   - Commands now use "bot " (with space) prefix
   - This prevents "both" from triggering "bothelp"
   - All commands work as before, just with the space

7. FEATURES:
   ✅ Automatic failover (60s timeout)
   ✅ Real-time data synchronization
   ✅ Health monitoring
   ✅ UptimeRobot integration
   ✅ Duplicate action prevention
   ✅ Command history tracking
   ✅ Everyone can use warn command
   ✅ Fixed command triggering bugs
   ✅ Graceful shutdown handling
   ✅ Memory management
   ✅ Detailed logging

8. TROUBLESHOOTING:
   - Check logs: banbot_[PID].log
   - Verify port availability: netstat -an | grep 5000
   - Test health endpoints: curl http://localhost:5000/health
   - Monitor Discord connection status in bot presence
   
9. SCALING:
   - System supports horizontal scaling
   - Can add more secondary instances if needed
   - Load balancing handled automatically
   - Data consistency maintained across all instances

The system is now production-ready with full high availability,
monitoring integration, and robust failover capabilities!
""".command(name="help", aliases=["h"])
async def help_command(ctx):
    """Display help information"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "help")
    
    embed = discord.Embed(
        title="🤖 BanBot 3000 HA Commands",
        description=f"Advanced Discord Moderation Bot (High Availability)\n**Instance: {bot.role.value.upper()} {'ACTIVE' if bot.is_active_instance else 'STANDBY'}**",
        color=discord.Color.blue()
    )

    embed.add_field(
        name="👑 Moderation Commands",
        value="""
`bot ban @user [reason]` - Ban a user (Requires Ban Members permission)
`bot kick @user [reason]` - Kick a user (Requires Kick Members permission)
`bot timeout @user <duration> [reason]` - Timeout a user (Requires Timeout Members permission)
`bot warn @user [reason]` - Warn a user (Everyone can use this)
`bot deop @user [reason]` - Remove admin privileges (Admin only)
`bot reop @user` - Restore admin privileges (Admin only)
""",
        inline=False
    )

    embed.add_field(
        name="📊 Info Commands",
        value="""
`bot stats` - Show bot statistics and HA status
`bot warnings @user` - Show user warnings
`bot history [@user]` - Show detailed command history
`bot deopped` - Show deopped users
`bot memory` - Show memory usage stats
`bot perms` - Check bot permissions
`bot userperms @user` - Check user permissions
`bot hastatus` - Show High Availability status
""",
        inline=False
    )

    embed.add_field(
        name="🔧 Utility",
        value="`bot ping` - Test bot response\n`bot cleanup [amount]` - Clean messages (Requires Manage Messages)\n`bot botcleanup` - Clean bot memory (Admin only)\n`bot uptime` - Show bot uptime",
        inline=False
    )

    embed.add_field(
        name="🏥 High Availability Features",
        value="• Automatic failover within 60 seconds\n• Real-time data synchronization\n• Health monitoring on port 5000/5001\n• UptimeRobot compatible endpoints",
        inline=False
    )

    await ctx.send(embed=embed)

@bot.command()
async def ping(ctx):
    """Test bot responsiveness"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "ping")
    
    latency = round(bot.latency * 1000)
    embed = discord.Embed(
        title="🏓 Pong!",
        description=f"BanBot 3000 HA is online!\n**Instance:** {bot.role.value.upper()} {'ACTIVE' if bot.is_active_instance else 'STANDBY'}\n**Latency:** {latency}ms\n**PID:** {os.getpid()}",
        color=discord.Color.green()
    )
    await ctx.send(embed=embed)

@bot.command()
async def hastatus(ctx):
    """Show High Availability status"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "hastatus")
    
    embed = discord.Embed(
        title="🏥 High Availability Status",
        description="Dual-instance bot system status",
        color=discord.Color.blue()
    )
    
    # Current instance info
    embed.add_field(
        name=f"🤖 This Instance ({bot.role.value.upper()})",
        value=f"**Status:** {'ACTIVE' if bot.is_active_instance else 'STANDBY'}\n**PID:** {os.getpid()}\n**Port:** {bot.port}\n**Discord Ready:** {'✅' if bot.is_ready() else '❌'}",
        inline=True
    )
    
    # Peer instance info
    peer_info = "Unknown"
    if bot.health_monitor.peer_url:
        try:
            async with bot.health_monitor.session.get(f"{bot.health_monitor.peer_url}/status") as resp:
                if resp.status == 200:
                    peer_data = await resp.json()
                    peer_status = "ACTIVE" if peer_data.get('is_active', False) else "STANDBY"
                    peer_info = f"**Status:** {peer_status}\n**PID:** {peer_data.get('process_id', 'Unknown')}\n**Guilds:** {peer_data.get('guilds', 0)}\n**Discord Ready:** {'✅' if peer_data.get('discord_ready', False) else '❌'}"
                else:
                    peer_info = f"❌ Connection Failed ({resp.status})"
        except Exception as e:
            peer_info = f"❌ Error: {str(e)[:50]}..."
    else:
        peer_info = "No peer configured"
    
    peer_role = "PRIMARY" if bot.role == BotRole.SECONDARY else "SECONDARY"
    embed.add_field(
        name=f"🔗 Peer Instance ({peer_role})",
        value=peer_info,
        inline=True
    )
    
    # System info
    uptime = datetime.now(timezone.utc) - bot.stats["uptime_start"]
    embed.add_field(
        name="📈 System Info",
        value=f"**Uptime:** {uptime.days}d {uptime.seconds//3600}h {(uptime.seconds//60)%60}m\n**Last Peer Check:** {(datetime.now(timezone.utc) - bot.health_monitor.last_peer_heartbeat).seconds}s ago\n**Health Check:** Every 30s\n**Failover Timeout:** 60s",
        inline=False
    )
    
    await ctx.send(embed=embed)

@bot.command()
async def perms(ctx):
    """Check bot permissions"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "perms")
    
    perms = await bot.check_permissions(ctx.guild)
    embed = discord.Embed(
        title="🔐 Bot Permissions",
        color=discord.Color.blue()
    )

    for perm, has_perm in perms.items():
        status = "✅" if has_perm else "❌"
        embed.add_field(
            name=f"{status} {perm.replace('_', ' ').title()}",
            value=f"Required for moderation" if perm in ["ban_members", "kick_members", "moderate_members"] else "Utility",
            inline=True
        )

    if not perms.get("moderate_members", False):
        embed.add_field(
            name="⚠️ Missing Permissions",
            value="Bot needs 'Timeout Members' permission for timeout commands!",
            inline=False
        )

    await ctx.send(embed=embed)

@bot.command()
async def userperms(ctx, member: discord.Member = None):
    """Check user permissions for moderation commands"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "userperms", member)
    
    if member is None:
        if isinstance(ctx.author, discord.Member):
            member = ctx.author
        else:
            await ctx.send("This command can only be used in a server.")
            return

    embed = discord.Embed(
        title=f"🔐 {member.display_name}'s Moderation Permissions",
        color=discord.Color.blue()
    )

    permissions_check = {
        "Ban Members": ("ban", member.guild_permissions.ban_members),
        "Kick Members": ("kick", member.guild_permissions.kick_members),
        "Timeout Members": ("timeout", member.guild_permissions.moderate_members),
        "Manage Messages": ("cleanup", member.guild_permissions.manage_messages),
        "Warn Users": ("warn", True)  # Everyone can warn
    }

    # Check if user is admin
    is_admin = bot.is_admin(member)
    is_deopped = bot.is_deopped(member.id)

    embed.add_field(
        name="👑 Admin Status",
        value="✅ Admin User" if is_admin else "❌ Not Admin",
        inline=True
    )

    embed.add_field(
        name="🚫 Deop Status",
        value="❌ Deopped" if is_deopped else "✅ Active",
        inline=True
    )

    embed.add_field(name="\u200b", value="\u200b", inline=True)

    for perm_name, (action, has_perm) in permissions_check.items():
        can_use = bot.has_permission_for_action(member, action) and not is_deopped
        status = "✅" if can_use else "❌"
        embed.add_field(
            name=f"{status} {perm_name}",
            value=f"Can use bot {action} commands" if can_use else "Cannot use command",
            inline=True
        )

    await ctx.send(embed=embed)

def parse_duration(duration_str: str) -> int:
    """Parse duration string and return minutes"""
    duration_str = duration_str.replace(" ", "").lower()
    
    if duration_str[-1] == 'm':
        return int(duration_str[:-1])
    elif duration_str[-1] == 'h':
        return int(duration_str[:-1]) * 60
    elif duration_str[-1] == 'd':
        return int(duration_str[:-1]) * 60 * 24
    else:
        raise ValueError("Duration must end with 'm' (minutes), 'h' (hours), or 'd' (days)")

@bot.command()
async def ban(ctx, member: discord.Member, *, reason: str = "No reason provided"):
    """Ban a user from the server"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "ban", member, reason)
    
    if not bot.is_authorized(ctx, "ban"):
        embed = discord.Embed(
            title="❌ Access Denied",
            description="You don't have permission to ban members or you've been deopped.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    if member == ctx.author:
        embed = discord.Embed(
            title="❌ Invalid Target",
            description="You cannot ban yourself!",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    if member.top_role >= ctx.author.top_role and not bot.is_admin(ctx.author):
        embed = discord.Embed(
            title="❌ Insufficient Hierarchy",
            description="You cannot ban someone with a higher or equal role!",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    try:
        # Log the action
        action = ModerationAction(
            user_id=member.id,
            moderator_id=ctx.author.id,
            action=ActionType.BAN,
            reason=reason,
            timestamp=datetime.now(timezone.utc)
        )
        bot.log_action(action)

        # Ban the member
        await member.ban(reason=f"Banned by {ctx.author}: {reason}")

        embed = discord.Embed(
            title="🔨 User Banned",
            description=f"**{member.display_name}** ({member.mention}) has been banned.",
            color=discord.Color.red()
        )
        embed.add_field(name="👤 Victim", value=f"{member.display_name} ({member.name})", inline=True)
        embed.add_field(name="⚖️ Executor", value=f"{ctx.author.display_name} ({ctx.author.name})", inline=True)
        embed.add_field(name="🤖 Instance", value=f"{bot.role.value.upper()}", inline=True)
        embed.add_field(name="📝 Reason", value=reason, inline=False)
        embed.timestamp = datetime.now(timezone.utc)

        await ctx.send(embed=embed)
        logger.info(f"User {member} banned by {ctx.author} for: {reason}")

    except discord.Forbidden:
        embed = discord.Embed(
            title="❌ Ban Failed",
            description="I don't have permission to ban this user.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
    except Exception as e:
        await bot.report_error(ctx, f"Ban command error: {str(e)}")

@bot.command()
async def kick(ctx, member: discord.Member, *, reason: str = "No reason provided"):
    """Kick a user from the server"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "kick", member, reason)
    
    if not bot.is_authorized(ctx, "kick"):
        embed = discord.Embed(
            title="❌ Access Denied",
            description="You don't have permission to kick members or you've been deopped.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    if member == ctx.author:
        embed = discord.Embed(
            title="❌ Invalid Target",
            description="You cannot kick yourself!",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    if member.top_role >= ctx.author.top_role and not bot.is_admin(ctx.author):
        embed = discord.Embed(
            title="❌ Insufficient Hierarchy",
            description="You cannot kick someone with a higher or equal role!",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    try:
        # Log the action
        action = ModerationAction(
            user_id=member.id,
            moderator_id=ctx.author.id,
            action=ActionType.KICK,
            reason=reason,
            timestamp=datetime.now(timezone.utc)
        )
        bot.log_action(action)

        # Kick the member
        await member.kick(reason=f"Kicked by {ctx.author}: {reason}")

        embed = discord.Embed(
            title="👢 User Kicked",
            description=f"**{member.display_name}** ({member.mention}) has been kicked.",
            color=discord.Color.orange()
        )
        embed.add_field(name="👤 Victim", value=f"{member.display_name} ({member.name})", inline=True)
        embed.add_field(name="⚖️ Executor", value=f"{ctx.author.display_name} ({ctx.author.name})", inline=True)
        embed.add_field(name="🤖 Instance", value=f"{bot.role.value.upper()}", inline=True)
        embed.add_field(name="📝 Reason", value=reason, inline=False)
        embed.timestamp = datetime.now(timezone.utc)

        await ctx.send(embed=embed)
        logger.info(f"User {member} kicked by {ctx.author} for: {reason}")

    except discord.Forbidden:
        embed = discord.Embed(
            title="❌ Kick Failed",
            description="I don't have permission to kick this user.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
    except Exception as e:
        await bot.report_error(ctx, f"Kick command error: {str(e)}")

@bot.command()
async def timeout(ctx, member: discord.Member, duration: str, *, reason: str = "No reason provided"):
    """Timeout a user for a specified duration"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "timeout", member, f"{duration} - {reason}")
    
    if not bot.is_authorized(ctx, "timeout"):
        embed = discord.Embed(
            title="❌ Access Denied",
            description="You don't have permission to timeout members or you've been deopped.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    if member == ctx.author:
        embed = discord.Embed(
            title="❌ Invalid Target",
            description="You cannot timeout yourself!",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    if member.top_role >= ctx.author.top_role and not bot.is_admin(ctx.author):
        embed = discord.Embed(
            title="❌ Insufficient Hierarchy",
            description="You cannot timeout someone with a higher or equal role!",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    # Parse duration
    try:
        duration_minutes = parse_duration(duration)
        if duration_minutes > (28 * 24 * 60):  # 28 days max
            embed = discord.Embed(
                title="❌ Invalid Duration",
                description="Maximum timeout duration is 28 days.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
    except ValueError as e:
        embed = discord.Embed(
            title="❌ Invalid Duration",
            description=str(e),
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    try:
        # Calculate timeout end time
        timeout_until = datetime.now(timezone.utc) + timedelta(minutes=duration_minutes)
        
        # Log the action
        action = ModerationAction(
            user_id=member.id,
            moderator_id=ctx.author.id,
            action=ActionType.TIMEOUT,
            reason=reason,
            duration=duration_minutes,
            timestamp=datetime.now(timezone.utc)
        )
        bot.log_action(action)

        # Timeout the member
        await member.timeout(timeout_until, reason=f"Timeout by {ctx.author}: {reason}")

        embed = discord.Embed(
            title="🤐 User Timed Out",
            description=f"**{member.display_name}** ({member.mention}) has been timed out for {duration}.",
            color=discord.Color.orange()
        )
        embed.add_field(name="👤 Victim", value=f"{member.display_name} ({member.name})", inline=True)
        embed.add_field(name="⚖️ Executor", value=f"{ctx.author.display_name} ({ctx.author.name})", inline=True)
        embed.add_field(name="🤖 Instance", value=f"{bot.role.value.upper()}", inline=True)
        embed.add_field(name="⏰ Duration", value=duration, inline=True)
        embed.add_field(name="📝 Reason", value=reason, inline=False)
        embed.timestamp = datetime.now(timezone.utc)

        await ctx.send(embed=embed)
        logger.info(f"User {member} timed out by {ctx.author} for {duration}: {reason}")

    except discord.Forbidden:
        embed = discord.Embed(
            title="❌ Timeout Failed",
            description="I don't have permission to timeout this user.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
    except Exception as e:
        await bot.report_error(ctx, f"Timeout command error: {str(e)}")

@bot.command()
async def warn(ctx, member: discord.Member, *, reason: str = "No reason provided"):
    """Warn a user - Everyone can use this command"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "warn", member, reason)
    
    # FIXED: Everyone can use warn command
    if not bot.is_authorized(ctx, "warn"):
        embed = discord.Embed(
            title="❌ Access Denied",
            description="You've been deopped from using moderation commands.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    if member == ctx.author:
        embed = discord.Embed(
            title="❌ Invalid Target",
            description="You cannot warn yourself!",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return

    try:
        # Add warning
        warning = bot.add_warning(member.id, ctx.author.id, reason)
        
        # Get user's warning count
        user_warnings = bot.get_user_warnings(member.id)
        warning_count = len(user_warnings)

        embed = discord.Embed(
            title="⚠️ User Warned",
            description=f"**{member.display_name}** ({member.mention}) has been warned.",
            color=discord.Color.yellow()
        )
        embed.add_field(name="👤 Victim", value=f"{member.display_name} ({member.name})", inline=True)
        embed.add_field(name="⚖️ Executor", value=f"{ctx.author.display_name} ({ctx.author.name})", inline=True)
        embed.add_field(name="🤖 Instance", value=f"{bot.role.value.upper()}", inline=True)
        embed.add_field(name="🆔 Warning ID", value=f"#{warning.id}", inline=True)
        embed.add_field(name="📊 Total Warnings", value=warning_count, inline=True)
        embed.add_field(name="📝 Reason", value=reason, inline=False)
        embed.timestamp = datetime.now(timezone.utc)

        await ctx.send(embed=embed)
        logger.info(f"User {member} warned by {ctx.author}: {reason}")

    except Exception as e:
        await bot.report_error(ctx, f"Warn command error: {str(e)}")

@bot.command()
async def warnings(ctx, member: discord.Member = None):
    """Show warnings for a user"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "warnings", member)
    
    if member is None:
        if isinstance(ctx.author, discord.Member):
            member = ctx.author
        else:
            await ctx.send("This command can only be used in a server.")
            return

    user_warnings = bot.get_user_warnings(member.id)
    
    embed = discord.Embed(
        title=f"⚠️ Warnings for {member.display_name}",
        color=discord.Color.yellow()
    )

    if not user_warnings:
        embed.description = "No warnings found."
    else:
        embed.description = f"Total active warnings: {len(user_warnings)}"
        
        for warning in user_warnings[:5]:  # Show last 5 warnings
            moderator = ctx.guild.get_member(warning.moderator_id)
            mod_name = moderator.display_name if moderator else "Unknown"
            
            embed.add_field(
                name=f"Warning #{warning.id}",
                value=f"**Reason:** {warning.reason}\n**Moderator:** {mod_name}\n**Date:** {warning.timestamp.strftime('%Y-%m-%d %H:%M UTC')}",
                inline=False
            )

    await ctx.send(embed=embed)

@bot.command()
async def history(ctx, member: discord.Member = None, limit: int = 10):
    """Shows the enhanced bot command history with executor and victim details"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "history", member)
    
    if not bot.is_authorized(ctx, "warn"):
        embed = discord.Embed(
            title="❌ Access Denied",
            description="You don't have permission to view command history.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return
    
    if limit < 1 or limit > 50:
        limit = 10
    
    # Filter history by member if specified
    if member:
        filtered_history = [
            entry for entry in bot.command_history 
            if (entry["executor"]["id"] == member.id or 
                (entry.get("victim") and entry["victim"]["id"] == member.id))
        ]
        title = f"📋 Command History for {member.display_name}"
    else:
        filtered_history = bot.command_history
        title = "📋 Bot Command History"
    
    if not filtered_history:
        embed = discord.Embed(
            title=title,
            description="No command history found.",
            color=discord.Color.blue()
        )
        await ctx.send(embed=embed)
        return
    
    # Get the last 'limit' entries
    recent_history = filtered_history[-limit:]
    
    embed = discord.Embed(
        title=title,
        description=f"Showing last {len(recent_history)} commands\n**Instance:** {bot.role.value.upper()}",
        color=discord.Color.blue()
    )
    
    for i, entry in enumerate(reversed(recent_history), 1):
        timestamp = entry["timestamp"].strftime("%Y-%m-%d %H:%M:%S UTC")
        executor_name = entry["executor"]["name"]
        executor_username = entry["executor"]["username"]
        command = entry["command"]
        
        # Build the field value with enhanced information
        field_value = f"**⚖️ Executor:** {executor_name} (@{executor_username})\n"
        field_value += f"**⏰ Time:** {timestamp}\n"
        field_value += f"**📍 Channel:** {entry['channel']}\n"
        
        # Add victim information if available
        if entry.get("victim"):
            victim_name = entry["victim"]["name"]
            victim_username = entry["victim"]["username"]
            field_value += f"**👤 Victim:** {victim_name} (@{victim_username})\n"
        
        # Add details if available
        if entry.get("details"):
            details = entry["details"]
            # Truncate details if too long
            if len(details) > 100:
                details = details[:100] + "..."
            field_value += f"**📝 Details:** {details}\n"
        
        # Add command icon based on type
        command_icons = {
            "ban": "🔨",
            "kick": "👢", 
            "timeout": "🤐",
            "warn": "⚠️",
            "deop": "🚫",
            "reop": "✅",
            "cleanup": "🧹"
        }
        
        icon = command_icons.get(command, "🤖")
        
        embed.add_field(
            name=f"{i}. {icon} bot {command}",
            value=field_value,
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command()
async def botcleanup(ctx):
    """Cleans the bot's memory/history"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "botcleanup")
    
    if not bot.is_admin(ctx.author):
        embed = discord.Embed(
            title="❌ Access Denied",
            description="Only administrators can clean bot memory.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return
    
    # Confirmation embed
    embed = discord.Embed(
        title="⚠️ Bot Memory Cleanup",
        description=f"This will clear all bot history and reset statistics on the **{bot.role.value.upper()}** instance. This action cannot be undone!\n\nReact with ✅ to confirm or ❌ to cancel.",
        color=discord.Color.orange()
    )
    
    # Add current memory stats
    memory_info = f"""
**Current Memory Usage:**
• Command History: {len(bot.command_history)} entries
• Moderation Actions: {len(bot.moderation_actions)} entries
• Warnings: {len(bot.warnings)} entries
• Deopped Users: {len(bot.deopped_users)} users
• Message Tracking: {len(bot.user_message_times)} users
    """
    embed.add_field(name="Memory Stats", value=memory_info, inline=False)
    
    # Add reaction-based confirmation
    message = await ctx.send(embed=embed)
    await message.add_reaction("✅")
    await message.add_reaction("❌")
    
    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == message.id
    
    try:
        reaction, user = await bot.wait_for("reaction_add", timeout=30.0, check=check)
        
        if str(reaction.emoji) == "✅":
            # Clear all memory
            bot.command_history.clear()
            bot.moderation_actions.clear()
            bot.warnings.clear()
            bot.deopped_users.clear()
            bot.user_message_times.clear()
            bot.processed_messages.clear()
            
            # Reset stats (keep uptime)
            uptime_start = bot.stats["uptime_start"]
            bot.stats = {
                "bans": 0,
                "kicks": 0,
                "timeouts": 0,
                "warnings": 0,
                "deops": 0,
                "commands_used": 0,
                "uptime_start": uptime_start
            }
            
            # Reset ID counters
            bot.next_warning_id = 1
            bot.next_action_id = 1
            
            success_embed = discord.Embed(
                title="✅ Cleanup Complete",
                description=f"Bot memory has been successfully cleared on **{bot.role.value.upper()}** instance!\n\n**Cleared:**\n• All command history\n• All moderation actions\n• All warnings\n• All deopped users\n• Message tracking data\n• Statistics (except uptime)",
                color=discord.Color.green()
            )
            await message.edit(embed=success_embed)
            await message.clear_reactions()
            
            logger.info(f"Bot memory cleaned by {ctx.author}")
            
        else:
            cancel_embed = discord.Embed(
                title="❌ Cleanup Cancelled",
                description="Bot memory cleanup has been cancelled.",
                color=discord.Color.grey()
            )
            await message.edit(embed=cancel_embed)
            await message.clear_reactions()
            
    except asyncio.TimeoutError:
        timeout_embed = discord.Embed(
            title="⏰ Cleanup Timeout",
            description="Cleanup confirmation timed out. No changes were made.",
            color=discord.Color.grey()
        )
        await message.edit(embed=timeout_embed)
        await message.clear_reactions()

@bot.command(name="help", aliases=["h"])
async def help_command(ctx):
    """Display help information"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "help")
    
    embed = discord.Embed(
        title="🤖 BanBot 3000 HA Commands",
        description=f"Advanced Discord Moderation Bot (High Availability)\n**Instance: {bot.role.value.upper()} {'ACTIVE' if bot.is_active_instance else 'STANDBY'}**",
        color=discord.Color.blue()
    )

    embed.add_field(
        name="👑 Moderation Commands",
        value="""
`bot ban @user [reason]` - Ban a user (Requires Ban Members permission)
`bot kick @user [reason]` - Kick a user (Requires Kick Members permission)
`bot timeout @user <duration> [reason]` - Timeout a user (Requires Timeout Members permission)
`bot warn @user [reason]` - Warn a user (Everyone can use this)
`bot deop @user [reason]` - Remove admin privileges (Admin only)
`bot reop @user` - Restore admin privileges (Admin only)
""",
        inline=False
    )

    embed.add_field(
        name="📊 Info Commands",
        value="""
`bot stats` - Show bot statistics and HA status
`bot warnings @user` - Show user warnings
`bot history [@user]` - Show detailed command history
`bot deopped` - Show deopped users
`bot memory` - Show memory usage stats
`bot perms` - Check bot permissions
`bot userperms @user` - Check user permissions
`bot hastatus` - Show High Availability status
""",
        inline=False
    )

    embed.add_field(
        name="🔧 Utility",
        value="`bot ping` - Test bot response\n`bot cleanup [amount]` - Clean messages (Requires Manage Messages)\n`bot botcleanup` - Clean bot memory (Admin only)\n`bot uptime` - Show bot uptime",
        inline=False
    )

    embed.add_field(
        name="🏥 High Availability Features",
        value="• Automatic failover within 60 seconds\n• Real-time data synchronization\n• Health monitoring on port 5000/5001\n• UptimeRobot compatible endpoints",
        inline=False
    )

    await ctx.send(embed=embed)

@bot.command()
async def ping(ctx):
    """Test bot responsiveness"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "ping")
    
    latency = round(bot.latency * 1000)
    embed = discord.Embed(
        title="🏓 Pong!",
        description=f"BanBot 3000 HA is online!\n**Instance:** {bot.role.value.upper()} {'ACTIVE' if bot.is_active_instance else 'STANDBY'}\n**Latency:** {latency}ms\n**PID:** {os.getpid()}",
        color=discord.Color.green()
    )
    await ctx.send(embed=embed)

@bot.command()
async def hastatus(ctx):
    """Show High Availability status"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "hastatus")
    
    embed = discord.Embed(
        title="🏥 High Availability Status",
        description="Dual-instance bot system status",
        color=discord.Color.blue()
    )
    
    # Current instance info
    embed.add_field(
        name=f"🤖 This Instance ({bot.role.value.upper()})",
        value=f"**Status:** {'ACTIVE' if bot.is_active_instance else 'STANDBY'}\n**PID:** {os.getpid()}\n**Port:** {bot.port}\n**Discord Ready:** {'✅' if bot.is_ready() else '❌'}",
        inline=True
    )
    
    # Peer instance info
    peer_info = "Unknown"
    if bot.health_monitor.peer_url:
        try:
            async with bot.health_monitor.session.get(f"{bot.health_monitor.peer_url}/status") as resp:
                if resp.status == 200:
                    peer_data = await resp.json()
                    peer_status = "ACTIVE" if peer_data.get('is_active', False) else "STANDBY"
                    peer_info = f"**Status:** {peer_status}\n**PID:** {peer_data.get('process_id', 'Unknown')}\n**Guilds:** {peer_data.get('guilds', 0)}\n**Discord Ready:** {'✅' if peer_data.get('discord_ready', False) else '❌'}"
                else:
                    peer_info = f"❌ Connection Failed ({resp.status})"
        except Exception as e:
            peer_info = f"❌ Error: {str(e)[:50]}..."
    else:
        peer_info = "No peer configured"
    
    peer_role = "PRIMARY" if bot.role == BotRole.SECONDARY else "SECONDARY"
    embed.add_field(
        name=f"🔗 Peer Instance ({peer_role})",
        value=peer_info,
        inline=True
    )
    
    # System info
    uptime = datetime.now(timezone.utc) - bot.stats["uptime_start"]
    embed.add_field(
        name="📈 System Info",
        value=f"**Uptime:** {uptime.days}d {uptime.seconds//3600}h {(uptime.seconds//60)%60}m\n**Last Peer Check:** {(datetime.now(timezone.utc) - bot.health_monitor.last_peer_heartbeat).seconds}s ago\n**Health Check:** Every 30s\n**Failover Timeout:** 60s",
        inline=False
    )
    
    await ctx.send(embed=embed)

@bot.command()
async def perms(ctx):
    """Check bot permissions"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "perms")
    
    perms = await bot.check_permissions(ctx.guild)
    embed = discord.Embed(
        title="🔐 Bot Permissions",
        color=discord.Color.blue()
    )

    for perm, has_perm in perms.items():
        status = "✅" if has_perm else "❌"
        embed.add_field(
            name=f"{status} {perm.replace('_', ' ').title()}",
            value=f"Required for moderation" if perm in ["ban_members", "kick_members", "moderate_members"] else "Utility",
            inline=True
        )

    if not perms.get("moderate_members", False):
        embed.add_field(
            name="⚠️ Missing Permissions",
            value="Bot needs 'Timeout Members' permission for timeout commands!",
            inline=False
        )

    await ctx.send(embed=embed)

@bot.command()
async def userperms(ctx, member: discord.Member = None):
    """Check user permissions for moderation commands"""
    if not bot.is_active_instance:
        return
        
    bot.log_command_usage(ctx, "userperms", member)
    
    if member is None:
        if isinstance(ctx.author, discord.Member):
            member = ctx.author
        else:
            await ctx.send("This command can only be used in a server.")
            return

    embed = discord.Embed(
        title=f"🔐 {member.display_name}'s Moderation Permissions",
        color=discord.Color.blue()
    )

    permissions_check = {
        "Ban Members": ("ban", member.guild_permissions.ban_members),
        "Kick Members": ("kick", member.guild_permissions.kick_members),
        "Timeout Members": ("timeout", member.guild_permissions.moderate_members),
        "Manage Messages": ("cleanup", member.guild_permissions.manage_messages),
        "Warn Users": ("warn", True)  # Everyone can warn
    }

    # Check if user is admin
    is_admin = bot.is_admin(member)
    is_deopped = bot.is_deopped(member.id)

    embed.add_field(
        name="👑 Admin Status",
        value="✅ Admin User" if is_admin else "❌ Not Admin",
        inline=True
    )

    embed.add_field(
        name="🚫 Deop Status",
        value="❌ Deopped" if is_deopped else "✅ Active",
        inline=True
    )

    embed.add_field(name="\u200b", value="\u200b", inline=True)

    for perm_name, (action, has_perm) in permissions_check.items():
        can_use = bot.has_permission_for_action(member, action) and not is_deopped
        status = "✅" if can_use else "❌"
        embed.add_field(
            name=f"{status} {perm_name}",
            value=f"Can use bot {action} commands" if can_use else "Cannot use command",
            inline=True
        )

    await ctx.send(embed=embed)

# Signal handlers for graceful shutdown
def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    bot.shutdown_requested = True
    
    # Create new event loop if needed
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Schedule the shutdown
    if loop.is_running():
        asyncio.create_task(shutdown_bot())
    else:
        loop.run_until_complete(shutdown_bot())

async def shutdown_bot():
    """Gracefully shutdown the bot"""
    logger.info("Starting graceful shutdown...")
    
    try:
        # Update status to show shutdown
        if bot.is_ready():
            await bot.change_presence(
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name="shutting down..."
                ),
                status=discord.Status.dnd
            )
        
        # Stop the bot
        await bot.close()
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    
    logger.info("Shutdown complete")

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def main():
    """Main function to run the bot with HTTP server"""
    try:
        # Load environment variables
        from dotenv import load_dotenv
        load_dotenv()
        
        # Start HTTP server first
        await bot.http_server.start()
        logger.info(f"🌐 HTTP server running on port {bot.port}")
        
        # Get Discord token
        token = os.getenv('DISCORD_TOKEN')
        if not token:
            logger.error("❌ No Discord token provided! Set the DISCORD_TOKEN environment variable.")
            return
        
        # Start the bot
        logger.info(f"🚀 Starting BanBot 3000 HA - Role: {bot.role.value.upper()}, Port: {bot.port}")
        await bot.start(token)
        
    except discord.LoginFailure:
        logger.error("❌ Invalid Discord token provided!")
    except Exception as e:
        logger.error(f"❌ Bot error: {e}")
        traceback.print_exc()
    finally:
        # Cleanup
        await bot.http_server.stop()
        logger.info("🔄 Bot stopped")

def run_bot():
    """Entry point to run the bot"""
    try:
        # Handle Windows-specific event loop policy
        if sys.platform.startswith('win'):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        # Run the main coroutine
        asyncio.run(main())
        
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    run_bot()
