import os
import logging
import asyncio
import sys
from threading import Thread
import signal
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global bot instance
bot_instance = None
shutdown_flag = False

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global shutdown_flag
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_flag = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def run_simple_http_server():
    """Run a simple HTTP server for health checks"""
    try:
        from flask import Flask
        app = Flask(__name__)

        @app.route('/')
        def home():
            return "BanBot 3000 HA is alive!", 200

        @app.route('/ping')
        def ping():
            return "pong", 200

        @app.route('/health')
        def health():
            global bot_instance
            status = {
                "status": "online",
                "bot_ready": bot_instance.is_ready() if bot_instance else False,
                "role": bot_instance.role.value if bot_instance else "unknown",
                "active": bot_instance.is_active_instance if bot_instance else False
            }
            return status, 200

        # Get port from environment (Railway provides PORT)
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
        
    except Exception as e:
        logger.error(f"HTTP server error: {e}")

def main():
    """Main entry point"""
    global bot_instance, shutdown_flag
    
    try:
        # Load environment variables
        from dotenv import load_dotenv
        load_dotenv()
        
        # Check for Discord token
        token = os.getenv('DISCORD_TOKEN')
        if not token:
            logger.error("❌ No Discord token found! Please set DISCORD_TOKEN environment variable.")
            return
        
        logger.info("🚀 Starting BanBot 3000 HA...")
        
        # Import bot after we know we have a token
        from bot import BanBot3000HA, BotRole, determine_instance_role
        
        # Start HTTP server in background thread
        http_thread = Thread(target=run_simple_http_server, daemon=True)
        http_thread.start()
        logger.info("🌐 HTTP server started")
        
        # Determine instance role (for Railway, we'll just use PRIMARY)
        # In a true HA setup, you'd deploy two instances
        role = BotRole.PRIMARY
        port = int(os.environ.get('PORT', 5000))
        
        # Create bot instance
        bot_instance = BanBot3000HA(role, port, None)  # No peer for single Railway deployment
        
        # Handle Windows event loop policy if needed
        if sys.platform.startswith('win'):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        # Run the bot
        async def run_bot():
            try:
                await bot_instance.start(token)
            except Exception as e:
                logger.error(f"Bot error: {e}")
                raise
            finally:
                if bot_instance:
                    await bot_instance.close()
        
        # Run the bot with proper cleanup
        asyncio.run(run_bot())
        
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        logger.info("🔄 Shutdown complete")

if __name__ == "__main__":
    main()
