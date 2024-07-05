import sys
import time
from loguru import logger
from multiprocessing import Process
# from base.config import PathConfig
from base.singleton import singleton
# from base.wechat_robot import WechatRobot


# class WechatHandler:
#     def __init__(self):
#         self.robot = WechatRobot(UtilsConfig.wechat_webhook_url)
#
#     def write(self, message):
#         self.robot.send_text(message)


class BaseLogger:
    """
    基础日志记录器类，封装了日志初始化、动态设置日志等级和资源清理功能。
    """

    def __init__(self,  log_file: str = None, level: str = "INFO", wechat_enabled: bool = False):
        self.log_file = log_file
        self.level = level.upper()
        self.wechat_enabled = wechat_enabled
        self.logger_instance = self.init_logger()

    def init_logger(self):
        """初始化并返回日志记录器实例"""
        # 移除默认
        logger.remove()

        # 日志格式设置
        log_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSSSSS}</green> | "
            "<cyan>{extra[name]}</cyan> | "
            "<cyan>{process.name}</cyan> | <cyan>{process.id}</cyan> | "
            "<magenta>{thread.name}</magenta> | <magenta>{thread.id}</magenta> | "
            "<level>{level}</level> | "
            "<cyan>{file.name}</cyan>:<cyan>{line}</cyan> | "  # 显示文件名和行号
            "\n"
            "<level>{message}</level>"
        )

        # 添加控制台处理器
        logger.add(
            sink=sys.stdout,
            level=self.level,
            format=log_format,
            colorize=True,
            backtrace=True,
            diagnose=True,
            filter=lambda record: record["extra"].get("destination") in ["console", "console_file", "console_wechat"],
        )

        # 如果指定了日志文件，则添加文件处理器
        if self.log_file:
            logger.add(
                sink=self.log_file,
                rotation="1 MB",
                retention="10 days",
                level=self.level,
                format=log_format,
                enqueue=True,  # 支持多进程安全记录
                filter=lambda record: record["extra"].get("destination") in ["file", "console_file"]
            )

        # 添加微信处理器（如果启用）
        # if self.wechat_enabled:
        #     wechat_handler = WechatHandler()
        #     logger.add(
        #         sink=wechat_handler.write,
        #         level=self.level,
        #         format=log_format,
        #         enqueue=True,  # 支持多进程安全记录
        #         filter=lambda record: record["extra"].get("destination") in ["wechat", "console_wechat"]
        #     )

        return logger

    @staticmethod
    def get_logger(name: str, destination: str = ""):
        # 判断destination是否为空
        if destination == "":
            return logger.bind(name=name)
        else:
            return logger.bind(name=name, destination=destination)

    def set_level(self, new_level: str):
        """
        动态设置所有已添加处理器的日志级别。
        注意：直接操作_loguru._logger.Logger._core.handlers_是非公开API的使用。
        """
        # self.level = new_level.upper()
        # for handler in logger._core.handlers.values():
        #     handler.setLevel(new_level)

    def __del__(self):
        """析构函数，自动清理资源"""
        self.logger_instance.remove()  # 清理日志处理器，释放资源


@singleton
class TraderLogger(BaseLogger):
    def __init__(self):
        super().__init__("", "DEBUG", True)


trader_log = TraderLogger().get_logger("trader_log", "console_file")
performance_log = TraderLogger().get_logger("performance_log", "console")


def task(process_id: int):
    for i in range(1):
        trader_log.info(f"Process {process_id} - Trader Log: iteration {i}")
        performance_log.error(f"Process {process_id} - Performance Log: iteration {i}")
        time.sleep(1)


if __name__ == "__main__":
    # VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    # user_input = input(
    #     "请输入新的日志级别（DEBUG, INFO, WARNING, ERROR, CRITICAL）或输入 'exit' 退出："
    # )
    # if user_input.lower() == "exit":
    #     exit()
    # elif user_input.upper() in VALID_LEVELS:
    #     # trader_log.set_level(user_input.upper())
    #     # performance_log.set_level(user_input.upper())
    #     print(f"日志级别已设置为 {user_input}")
    # else:
    #     print(
    #         f"无效的日志级别: {user_input}。请使用以下之一: {', '.join(VALID_LEVELS)} 或输入 'exit' 退出。"
    #     )

    # 创建多个进程执行任务
    processes = []
    for i in range(1):
        process_name = f"Process {i}"
        p = Process(target=task, args=(i,), name=process_name)
        processes.append(p)
        p.start()

    # 等待所有进程完成
    for p in processes:
        p.join()

    performance_log.info("All initial processes have finished.")
    # trader_log.warning("All initial processes have finished.")
    # trader_log.error("All initial processes have finished.")
    # performance_log.info("All initial processes have finished.")
    # performance_log.warning("All initial processes have finished.")
    # performance_log.error("All initial processes have finished.")
