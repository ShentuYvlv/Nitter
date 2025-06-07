import os
import sys
import argparse
import subprocess

def main():
    """
    在 Windows 环境下分别启动轮询引擎和 SSE 服务器
    """
    # 获取当前脚本所在目录的绝对路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="在 Windows 环境下启动推特实时推送系统")
    
    parser.add_argument(
        "--following-file",
        default="E:\\all code\\MESSAGE\\nitter\\config\\following_list.json",
        help="关注列表文件路径 (默认: following_list.json)"
    )
    
    parser.add_argument(
        "--nitter-instances",
        default="http://localhost:8080",
        help="Nitter实例URL，多个实例用逗号分隔 (默认: http://localhost:8080)"
    )
    
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="服务器主机地址 (默认: 127.0.0.1)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="服务器端口 (默认: 8000)"
    )
    
    parser.add_argument(
        "--debug-mode",
        action="store_true",
        help="启用调试模式，显示详细日志"
    )
    
    args = parser.parse_args()
    
    # 构建轮询引擎命令
    polling_engine_path = os.path.join(script_dir, "polling_engine.py")
    following_file_path = os.path.join(script_dir, args.following_file)
    
    polling_cmd = [
        sys.executable,
        polling_engine_path,
        "--following-file", following_file_path,
        "--nitter-instances", args.nitter_instances
    ]
    
    # 构建 SSE 服务器命令
    sse_server_path = os.path.join(script_dir, "sse_server.py")
    
    sse_cmd = [
        sys.executable,
        sse_server_path,
        "--host", args.host,
        "--port", str(args.port)
    ]
    
    # 如果启用调试模式，则使用新窗口显示输出
    if args.debug_mode:
        print("调试模式已启用，将在新窗口中显示详细日志...")
        
        # 使用 start 命令在新的命令窗口中启动轮询引擎
        polling_process = subprocess.Popen(
            f'start cmd /k "cd /d {script_dir} && {sys.executable} {polling_engine_path} --following-file {following_file_path} --nitter-instances {args.nitter_instances}"',
            shell=True
        )
        
        # 使用 start 命令在新的命令窗口中启动 SSE 服务器
        sse_process = subprocess.Popen(
            f'start cmd /k "cd /d {script_dir} && {sys.executable} {sse_server_path} --host {args.host} --port {args.port}"',
            shell=True
        )
    else:
        # 在后台启动轮询引擎，不显示窗口
        print("启动轮询引擎...")
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        
        polling_process = subprocess.Popen(
            polling_cmd,
            cwd=script_dir,
            startupinfo=startupinfo
        )
        
        # 在后台启动 SSE 服务器，不显示窗口
        print("启动 SSE 服务器...")
        sse_process = subprocess.Popen(
            sse_cmd,
            cwd=script_dir,
            startupinfo=startupinfo
        )
    
    print(f"推特实时推送系统已启动，请访问 http://{args.host}:{args.port}")
    print("注意：此窗口关闭后，系统将继续在后台运行")
    print("要停止系统，请在任务管理器中结束 Python 进程")

if __name__ == "__main__":
    if sys.platform != "win32":
        print("错误：此脚本仅适用于 Windows 环境")
        sys.exit(1)
    main() 