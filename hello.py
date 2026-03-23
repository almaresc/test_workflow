def greet(name: str) -> str:
    return f"Hello, {name}!"


def add(a: int, b: int) -> int:
    return a + b


def fibonacci(n: int) -> list:
    if n <= 0:
        return []
    if n == 1:
        return [0]
    fib = [0, 1]
    for i in range(2, n):
        fib.append(fib[i - 1] + fib[i - 2])
    return fib


if __name__ == "__main__":
    print(greet("Test workflow with copilot fix varie - test finale v20"))
    print(f"3 + 5 = {add(3, 5)} base base")
    print(f"Fibonacci(10) = {fibonacci(10)}")
