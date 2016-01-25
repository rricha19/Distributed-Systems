
import java.net.*;
import java.io.*;
import java.util.List;
import java.util.ArrayList;

public class OnlineBrokerServerHandlerThread extends Thread {
	
	
	static List<String> entry = new ArrayList<String>();
	private static Socket socket = null;	
	

	public OnlineBrokerServerHandlerThread(Socket socket) {
		super("BrokerServerHandlerThread");
		this.socket = socket;
		System.out.println("Created new Thread to handle client");
	}

	public void run() {

		boolean gotByePacket = false;

		try {

			/* stream to read from client */
			ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());
			BrokerPacket packetFromClient;
			
			
			/* stream to write back to client */
			ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());
			
			
			while (( packetFromClient = (BrokerPacket) fromClient.readObject()) != null) {
				String symbolGiven = "";
				Long quoteGiven = 0L;

				/* create a packet to send reply back to client */
				
				BrokerPacket packetToClient = new BrokerPacket();
				packetToClient.type = BrokerPacket.BROKER_NULL;
				
				/* process message */
				/* just echo in this example */
				if(packetFromClient.type == BrokerPacket.BROKER_REQUEST ||   
				   packetFromClient.type == BrokerPacket.EXCHANGE_ADD || 
				   packetFromClient.type == BrokerPacket.EXCHANGE_UPDATE || 
				   packetFromClient.type == BrokerPacket.EXCHANGE_REMOVE)
				{
					symbolGiven = packetFromClient.symbol;
					quoteGiven = packetFromClient.quote;
				
				if(packetFromClient.type == BrokerPacket.BROKER_REQUEST) {
				  
					System.out.println("From Client: " + symbolGiven);
					
						
					System.out.println("entered print quote: Symbol requested:" +symbolGiven);
					int foundAt = searchAL(symbolGiven);
					System.out.println("foundAt: "+foundAt);
					if(foundAt != -1){
						String[] split = entry.get(foundAt).split(" ");
						packetToClient.type = BrokerPacket.BROKER_QUOTE;
						packetToClient.symbol = symbolGiven;
						packetToClient.quote = Long.parseLong(split[1]);
					}
					else{
						packetToClient.type = BrokerPacket.BROKER_ERROR;
						packetToClient.error_code = BrokerPacket.ERROR_INVALID_SYMBOL;
						packetToClient.symbol = symbolGiven;
					}
						
				}

				/* If Exchange request */
				else if(packetFromClient.type == BrokerPacket.EXCHANGE_ADD) {
			
					System.out.println("Entered exchange ADD");
					if(searchAL(symbolGiven) == -1) { //not present
				       	entry.add(symbolGiven + " " + quoteGiven);
						System.out.println("Entry added: "+ symbolGiven + " , " + quoteGiven);
				       	packetToClient = sendExchangePacket(packetToClient,BrokerPacket.EXCHANGE_REPLY, 							packetFromClient.symbol,0L,BrokerPacket.BROKER_NULL);
					}
				 	else {
						packetToClient = sendExchangePacket (packetToClient, BrokerPacket.BROKER_ERROR, 							packetFromClient.symbol, 0L, BrokerPacket.ERROR_SYMBOL_EXISTS);
				     	}
				 }

				else if(packetFromClient.type == BrokerPacket.EXCHANGE_UPDATE) {
					System.out.println("Entered exchange Update");
					if(1L <= quoteGiven && quoteGiven <= 300L){
						int foundAt = searchAL(symbolGiven);
						if(foundAt != -1) { // present
				 			String newEntry = symbolGiven+ " " +quoteGiven;
							entry.set(foundAt, newEntry);
							System.out.println(entry.get(foundAt));				
							packetToClient = sendExchangePacket(packetToClient, BrokerPacket.EXCHANGE_REPLY, 								packetFromClient.symbol, quoteGiven,BrokerPacket.BROKER_NULL);
				       	}
				       	else {
							sendExchangePacket(packetToClient,BrokerPacket.BROKER_ERROR, 						 		packetFromClient.symbol, quoteGiven,BrokerPacket.ERROR_INVALID_SYMBOL);
						}
				   	}
				  	else {
  						sendExchangePacket(packetToClient,BrokerPacket.BROKER_ERROR,   						packetFromClient.symbol, quoteGiven, BrokerPacket.ERROR_OUT_OF_RANGE);			
				   	}
				}

				else if(packetFromClient.type == BrokerPacket.EXCHANGE_REMOVE) {
					System.out.println("Entered exchange Update");
					
					int foundAt = searchAL(symbolGiven);
					if(foundAt != -1) { // present
					       entry.remove(foundAt);
					       packetToClient = sendExchangePacket(packetToClient,BrokerPacket.EXCHANGE_REPLY, 							packetFromClient.symbol, 0L,BrokerPacket.BROKER_NULL);
					}	
				     	else {
						packetToClient = sendExchangePacket(packetToClient, BrokerPacket.BROKER_ERROR, 							packetFromClient.symbol, 0L,BrokerPacket.ERROR_INVALID_SYMBOL);
				     	}
				}

				 /* send reply back to client */
				   toClient.writeObject(packetToClient);
					
				   /* wait for next packet */
				   continue;
				}
			   	    
					
			     /* Sending an BROKER_NULL || BROKER_BYE means quit */
			    if(packetFromClient.type==BrokerPacket.BROKER_NULL || packetFromClient.type==BrokerPacket.BROKER_BYE) {
					gotByePacket = true;					

					packetToClient = new BrokerPacket();
					packetToClient.type = BrokerPacket.BROKER_BYE;
					//packetToClient.symbol = "Bye!";
					toClient.writeObject(packetToClient);
					break;
				}
				
			    /* if code comes here, there is an error in the packet */
			    System.err.println("ERROR: Unknown Broker_* packet!!");
			    System.exit(-1);
		     }
			
		     /* cleanup when client exits */
		     fromClient.close();
		     toClient.close();
		     socket.close();

		} catch (IOException e) {
			if(!gotByePacket)
				e.printStackTrace();
		} catch (ClassNotFoundException e) {
			if(!gotByePacket)
				e.printStackTrace();
		}
	}

	public static void setMachineEntries (List<String> entryInFile){
		OnlineBrokerServerHandlerThread.entry = entryInFile;
	}


    	private BrokerPacket sendExchangePacket(BrokerPacket packetToClient, int type, String symbol, Long quote, int error_code) throws 	 	IOException {
        packetToClient.type = type;
        packetToClient.symbol = symbol;
        packetToClient.quote = quote;
        packetToClient.error_code = error_code;
	 return packetToClient;
    	}

	public int searchAL(String symbol)
	{
		int i = 0;
		System.out.println("Entered searchAl");
		while(i<entry.size() && entry.get(i)!= null){
			String[] split = (entry.get(i)).split(" ");
			System.out.println(split[0]+ " " + split[1]);
			if(split[0].equalsIgnoreCase(symbol))
			{
				System.out.println("Returning i " + i);
				return i;
			}
		i++;			
		}
		System.out.println("Returning -1");
		return -1;
	}


	public static boolean register(BrokerLocation bl, String exchange, String hostname, int port){

		boolean registered = false;
		try{
			System.out.println("Entered register");
			Socket sock = new Socket(bl.broker_host, bl.broker_port);
			ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
			BrokerPacket packetToClient = new BrokerPacket();
			packetToClient.type = BrokerPacket.LOOKUP_REGISTER;
			packetToClient.exchange = exchange;
			packetToClient.num_locations = 1;
			packetToClient.locations = new BrokerLocation[] { new BrokerLocation(hostname, port) };
			System.out.println("ToClient= " + packetToClient.locations[0]);
			System.out.println("Host= " + packetToClient.locations[0].broker_host);
			System.out.println("Port= " + packetToClient.locations[0].broker_port);
			out.writeObject(packetToClient);

			BrokerPacket packetFromClient = (BrokerPacket) in.readObject();
			if(packetFromClient == null)		
			   System.out.println("FromClient NULL error");
			else
			   System.out.println("FromClient " + packetFromClient.type + " ex: " + packetFromClient.exchange);  				    System.out.println("locations "+ packetFromClient.locations[0]);

			if(packetFromClient.type == BrokerPacket.LOOKUP_REPLY){
				if(packetFromClient.locations[0].broker_host.equalsIgnoreCase(hostname) && 
				   packetFromClient.locations[0].broker_port == port){
					registered = true;
				}
			}	
			
			out.close();
			in.close();
			sock.close();
		}
		catch(IOException e){
			e.printStackTrace();			
		}
		catch(ClassNotFoundException e){
			e.printStackTrace();
		}
		return registered;
	}


}
