import java.net.*;
import java.io.*;

import java.util.List;
import java.util.ArrayList;

public class OnlineBroker {
	
	public static final String FILE = "nasdaq";	
	static List<String> entryInFile = new ArrayList<String>();
	public static ServerSocket serverSocket = null;


	public static void main(String[] args) throws IOException {
		boolean listening = true;
		BrokerLocation bl;
		String exchange = "";		

		try {
        		if(args.length == 4) {
        			serverSocket = new ServerSocket(Integer.parseInt(args[2]));
				bl = new BrokerLocation(args[0], Integer.parseInt(args[1]));
				exchange = args[3];
				String regHost = InetAddress.getLocalHost().getHostAddress();
				int regPort = Integer.parseInt(args[2]);
				boolean registered = true;
				OnlineBrokerServerHandlerThread.register(bl, exchange, regHost, regPort);
				if(!registered)
					System.out.println("Could not register!!");
        		} 
			else {
        			System.err.println("ERROR: Invalid arguments!");
        			System.exit(-1);
        		}
        	} catch (IOException e) {
           	System.err.println("ERROR: Could not listen on port!");
           	System.exit(-1);
        }

	String lineFromFile;
		
	try{
		FileReader fileReader = new FileReader(exchange);
		BufferedReader br = new BufferedReader(fileReader);
			
		System.out.println("\n Printing entries is in the file \n");
		while ((lineFromFile = br.readLine()) != null){
			System.out.println(lineFromFile);
			entryInFile.add(lineFromFile);
		}
		fileReader.close();
	}
	catch(Exception e){
		System.out.println("Error: "+ e.getMessage());
	}
	
	OnlineBrokerServerHandlerThread.setMachineEntries(entryInFile);


	Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
		try{
			System.out.println("Enters file writer");
			FileWriter fw = new FileWriter(FILE);
			BufferedWriter bw = new BufferedWriter(fw);
			for(int i=0; i< entryInFile.size(); i++){
				System.out.println("writing entry: "+ entryInFile.get(i));
				bw.write(entryInFile.get(i) + "\n");
			}
			bw.close();
		}
		catch(Exception e){
			System.out.println("Error: "+ e.getMessage());
		}
	}
	});

        while (listening) {
        	new OnlineBrokerServerHandlerThread(serverSocket.accept()).start();
        }

        serverSocket.close();
    }


	
}
