from pattern_search import state
from pattern_search.db import save_queue
from pattern_search.runner import main_loop


if __name__ == "__main__":
    try:
        state.send_notification("📡 Booted complete!")
        state.main_loop_running = True
        main_loop()
    except KeyboardInterrupt:
        print("👋 Caught Ctrl-C; shutting down…")
    finally:
        try:
            state.stop_event.set()
        except Exception:
            pass
        # Give the saver a brief chance to flush
        try:
            save_queue.join()
        except Exception:
            pass
        print("🧹 Cleanup done. Bye.")
