import os, sys, runpy
os.chdir(r'D:\feishu-claude-bot')
sys.path.insert(0, r'D:\feishu-claude-bot')
runpy.run_module('src.main_websocket', run_name='__main__')
