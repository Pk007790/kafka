package kafka.examples.streams.wordcount;

/**
 * Created by PravinKumar on 28/7/17.
 */
public class Hello {

    public static void main(String[] args) throws InterruptedException {
        String name = "Hello";
        System.out.println(name.getBytes().toString());

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.err.println("Helllllllo");
            }
        }));

        System.out.println("Coming here");
        Thread.sleep(50_000);
    }
}
