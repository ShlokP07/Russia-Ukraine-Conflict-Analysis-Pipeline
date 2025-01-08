#!/bin/bash

# Create necessary directories with proper paths
mkdir -p logs/chan_crawler
mkdir -p logs/reddit_crawler
mkdir -p pids

# Function to start a crawler
start_crawler() {
    local name=$1
    local script_dir=$2
    local script_file=$3
    local log_file="$(pwd)/logs/${name}/crawler.log"
    local error_file="$(pwd)/logs/${name}/error.log"
    local pid_file="$(pwd)/pids/${name}.pid"
    local abs_script_dir="$(pwd)/${script_dir}"
    local venv_python="${abs_script_dir}/venv/bin/python"

    # Check if already running
    if [ -f "$pid_file" ] && ps -p $(cat "$pid_file") > /dev/null; then
        echo "${name} is already running."
        return
    fi

    echo "Starting ${name}..."
    
    # Verify virtual environment exists
    if [ ! -f "$venv_python" ]; then
        echo "Error: Virtual environment Python not found at ${venv_python}"
        return 1
    fi
    
    # Change to the crawler directory
    cd "${script_dir}" || exit 1
    
    # Load environment variables
    if [ -f ".env" ]; then
        set -a
        source .env
        set +a
    fi

    echo "Using Python: ${venv_python}"
    echo "Current directory: $(pwd)"
    echo "Starting script: ${script_file}"
    
    # Start the crawler using the virtual environment's Python
    # Use full paths for everything
    PYTHONPATH="$(pwd)" nohup "${venv_python}" "$(pwd)/${script_file}" > "${log_file}" 2> "${error_file}" & 
    local pid=$!
    echo $pid > "${pid_file}"
    
    # Wait a moment to see if the process stays alive
    sleep 2
    if ps -p $pid > /dev/null; then
        echo "${name} started successfully. PID: $pid"
        echo "Log file: $log_file"
        echo "Error file: $error_file"
        tail -n 5 "${log_file}"
    else
        echo "${name} failed to start. Check logs at $error_file"
        cat "${error_file}"
        rm -f "${pid_file}"
    fi
    
    # Return to original directory
    cd - > /dev/null
}

# Function to stop a crawler
stop_crawler() {
    local name=$1
    local pid_file="pids/${name}.pid"

    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        if ps -p $pid > /dev/null; then
            echo "Stopping ${name}..."
            kill $pid
            sleep 2
            # Check if it's still running and force kill if necessary
            if ps -p $pid > /dev/null; then
                echo "Force stopping ${name}..."
                kill -9 $pid
            fi
            rm -f "$pid_file"
            echo "${name} stopped."
        else
            echo "${name} is not running."
            rm -f "$pid_file"
        fi
    else
        echo "${name} is not running."
    fi
}

# Function to check status
check_status() {
    local name=$1
    local pid_file="pids/${name}.pid"

    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        if ps -p $pid > /dev/null; then
            echo "${name} is running. PID: $pid"
            echo "Process info:"
            ps -f -p $pid
            echo "Recent logs:"
            tail -n 5 "logs/${name}/crawler.log"
            echo "Recent errors:"
            tail -n 5 "logs/${name}/error.log"
        else
            echo "${name} is not running (stale PID file)."
            rm -f "$pid_file"
        fi
    else
        echo "${name} is not running."
    fi
}

# Command line interface
case "$1" in
    start)
        start_crawler "chan_crawler" "chan_crawler" "chan_crawler.py"
        start_crawler "reddit_crawler" "reddit_crawler" "reddit_crawler.py"
        ;;
    stop)
        stop_crawler "chan_crawler"
        stop_crawler "reddit_crawler"
        ;;
    restart)
        stop_crawler "chan_crawler"
        stop_crawler "reddit_crawler"
        sleep 2
        start_crawler "chan_crawler" "chan_crawler" "chan_crawler.py"
        start_crawler "reddit_crawler" "reddit_crawler" "reddit_crawler.py"
        ;;
    status)
        check_status "chan_crawler"
        check_status "reddit_crawler"
        ;;
    logs)
        echo "=== Chan Crawler Logs ==="
        tail -n 20 logs/chan_crawler/crawler.log
        echo -e "\n=== Chan Crawler Errors ==="
        tail -n 20 logs/chan_crawler/error.log
        echo -e "\n=== Reddit Crawler Logs ==="
        tail -n 20 logs/reddit_crawler/crawler.log
        echo -e "\n=== Reddit Crawler Errors ==="
        tail -n 20 logs/reddit_crawler/error.log
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs}"
        exit 1
        ;;
esac

exit 0