package com.yuttasak.camel;

import org.apache.camel.main.Main;

public class Application {

    public static void main(String[] args) throws Exception {
        // RouteBuilder classes in this package are discovered automatically
        Main main = new Main(Application.class);
        main.run(args);
    }
}
