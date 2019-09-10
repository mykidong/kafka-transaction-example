package mykidong;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * Created by mykidong on 2019-09-10.
 */
public class SomeMain {

    public static void main(String[] args)
    {
        // args could be: --some.arg1 value1 --some.arg2 value2
        OptionParser parser = new OptionParser();
        parser.accepts("some.arg1").withRequiredArg().ofType(String.class);
        parser.accepts("some.arg2").withRequiredArg().ofType(String.class);

        OptionSet options = parser.parse(args);

        String arg1 = (String) options.valueOf("some.arg1");
        String arg2 = (String) options.valueOf("some.arg2");

        // .................
    }
}
