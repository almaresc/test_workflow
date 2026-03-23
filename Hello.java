import java.util.ArrayList;
import java.util.List;

public class Hello {

    public static String greet(String name) {
        return "Hello, " + name + "!";
    }

    public static int add(int a, int b) {
        return a + b;
    }

    public static List<Integer> fibonacci(int n) {
        List<Integer> fib = new ArrayList<>();
        if (n <= 0) {
            return fib;
        }
        fib.add(0);
        if (n == 1) {
            return fib;
        }
        fib.add(1);
        for (int i = 2; i < n; i++) {
            fib.add(fib.get(i - 1) + fib.get(i - 2));
        }
        return fib;
    }

    public static void main(String[] args) {
        System.out.println(greet("Test workflow with copilot fix varie - test finale v3"));
        System.out.println("3 + 5 = " + add(3, 5) + " base base");
        System.out.println("Fibonacci(10) = " + fibonacci(10));
    }
}
