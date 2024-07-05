# coding:utf-8
import functools
import timeit
import time
from base.log import performance_log

# Try to import the best available time measurement function
try:
    from time import process_time, time
except ImportError:
    from time import clock as process_time, time


class PerformanceTimer:
    """
    Class that provides a decorator for timing functions and logging the duration.
    """

    def __init__(self, enabled=True):
        self.enabled = enabled

    def __call__(self, func):
        """
        The method that makes this class a decorator.

        Args:
            func (Callable): The function to be wrapped

        Returns:
            Callable: The wrapped function with performance logging
        """

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not self.enabled:
                return func(*args, **kwargs)

            start_time = timeit.default_timer()
            start_cpu_time = process_time()

            exception_occurred = None
            result = None

            try:
                result = func(*args, **kwargs)
            except Exception as e:
                exception_occurred = e

            end_time = timeit.default_timer()
            end_cpu_time = process_time()

            # performance_log.debug(
            #     "function:%r consume %2.4f sec, cpu %2.4f sec. args %s, extra args %s"
            #     % (
            #         func.__qualname__,
            #         end_time - start_time,
            #         end_cpu_time - start_cpu_time,
            #         args[1:],
            #         kwargs,
            #     ),
            # )
            performance_log.debug(
                "function:%r consume %2.4f sec, cpu %2.4f sec."
                % (
                    func.__qualname__,
                    end_time - start_time,
                    end_cpu_time - start_cpu_time,
                ),
            )

            if exception_occurred is not None:
                raise exception_occurred

            return result

        return wrapper


# Create an instance of PerformanceTimer with debug logging enabled.
performance_timer = PerformanceTimer(enabled=True)


@performance_timer
def some_function_to_measure(x, y):
    # Function code here...
    time.sleep(x + y)


if __name__ == "__main__":
    some_function_to_measure(1, 2)
