package utils;

import java.util.HashMap;
import java.util.Map;

public class ArgumentContainer {
    Map<String,String> arguments = new HashMap<>();

    public ArgumentContainer(String [] args){
        if(args != null && args.length !=0){
            String[] localArguments = args;
            int argumentsLength = args.length;

            for(int argsIndex = 0 ; argsIndex < argumentsLength; ++ argsIndex){
                String[] keyValuePair = localArguments[argsIndex].split("=",2);
                arguments.put(keyValuePair[0],keyValuePair[1]);
            }
        }
    }

    public String getRequired(String name){
        if(arguments.containsKey(name)){
            return this.arguments.get(name);
        } else {
            throw new RuntimeException("Argument not found");

        }
    }
}
