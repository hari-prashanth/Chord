import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Node extends Thread 
{
	static String myIp;
	static int myNodeId;
	static int myIdSpaceStart;
	static int myIdSpaceEnd;
	static String mySuccessor;
	static String myPredecessor;	
	static int m; //(0 to 31)
	static List<String> myFiles;
	static String route;
	static String bsServerIp;
	static Boolean myFlag;
	static Boolean joinFlag;
	static Boolean changeFlag;
	static String route1;
	
	public int hashFunction(String value)
	{	 		
		int hash = 1;
		
		hash = value.hashCode();
		hash = hash% 32;
		
		if(hash < 0)
		{
			hash = -1 * hash;
			hash = hash% 32;
		}
		//System.out.println(hash);
		
		return hash;
		
	}
	
	public String forwardDirection(int inputHashValue)
	{
		int totalNodes = (int) (Math.pow(2, m) - 1);
		int halfofTotalNodes = totalNodes/2;
					 
		//System.out.println(totalNodes);
		//frwd to successor 
		if(inputHashValue > myIdSpaceEnd
				&& inputHashValue < (myIdSpaceEnd + halfofTotalNodes) %totalNodes)
		{	
			return "successor";
		}
		
		return "predecessor";
	}
	
	public Boolean isNodeInMyIdSpace(int newNodeId)
	{
		if(myFlag == true)
		{
			if(newNodeId >= myIdSpaceStart && newNodeId <= (Math.pow(2, m) - 1))
			{
				//System.out.println("return 1st true"); 
				return true;
			}
			
			else if(newNodeId >= 0 && newNodeId <= myIdSpaceEnd)
			{
				//System.out.println("return 1st true"); 
				//myFlag = false;
				changeFlag = true;
				//System.out.println("changing the changeFlag value to true");
				return true;
			}
			
			//System.out.println("first false");
			return false;
		}
		
		else
		{
			if(newNodeId >= myIdSpaceStart && newNodeId <= myIdSpaceEnd)
			{
				//System.out.println("return 2nd true");
				return true;
			}
			
			//System.out.println("return 2nd false");
			return false;
		}
	}
	
	public Boolean isFileInMyIdSpace(int fileHash) throws UnknownHostException
	{
		UpdateFilesList();
		
		if(myFlag == true)
		{
			if(fileHash >= myIdSpaceStart && fileHash <= (Math.pow(2, m) - 1))
			{
				 //System.out.println("return 1st true"); 
				return true;
			}
			
			else if(fileHash >= 0 && fileHash <= myIdSpaceEnd)
			{
				 //System.out.println("return 1st true"); 
				return true;
			}
			
			//System.out.println("first false");
			return false;
		}
		
		else
		{
			if(fileHash >= myIdSpaceStart && fileHash <= myIdSpaceEnd)
			{
				// System.out.println("return 2nd true");
				return true;
			}
			
			//System.out.println("return 2nd false");
			return false;
		}
	}
	
	public void PrintFilesList() throws UnknownHostException
	{
		myFiles.clear();
		
		File folder = new File("/home/stu4/s6/hr1652/DS/proj1/" + getMyIp() + "/");
		File[] filesList = folder.listFiles();
				
		if(filesList.length > 0)
		{
			System.out.println("The files present are ...");
			
			for(int i=0; i<filesList.length; i++)
			{
				if(filesList[i].isFile())
				{
					System.out.println(filesList[i].getName());
					myFiles.add(filesList[i].getName());
				}
			}
		}
		
		else
			System.out.println("There are no files!");
		
	}
	
	
	
	public void UpdateFilesList() throws UnknownHostException
	{
		myFiles.clear();
		
		File folder = new File("/home/stu4/s6/hr1652/DS/proj1/" + getMyIp() + "/");
		File[] filesList = folder.listFiles();
				
		if(filesList.length > 0)
		{
			//System.out.println("The files present are ...");
			
			for(int i=0; i<filesList.length; i++)
			{
				if(filesList[i].isFile())
				{
					myFiles.add(filesList[i].getName());
				}
				
			}
			
		}
	}
	

	public void joinHandler(String newNodeIp) throws IOException
	{
		int newNodeId = hashFunction(newNodeIp);
		
		if(isNodeInMyIdSpace(newNodeId))
		{
			//System.out.println("In myIdSpace");
			
			Socket newSocket = new Socket(newNodeIp, 9999);
			
			DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
			
			dos.writeUTF("joinResult");
			int newNodeIdSpaceStart = myIdSpaceStart;
			int newNodeIdSpaceEnd = newNodeId;
			
			dos.writeBoolean(changeFlag);
			dos.write(newNodeIdSpaceStart);
			dos.write(newNodeIdSpaceEnd);
			

			dos.writeUTF(myPredecessor);
			dos.writeUTF(myIp);
			myIdSpaceStart = newNodeId + 1;
			
			//System.out.println("id space is" + newNodeIdSpaceStart + " to " + newNodeIdSpaceEnd);
			UpdateFilesList();
			
			int filesCount = 0;
			
			for(int i=0; i< myFiles.size(); i++)
			{
				//if file does not belong to my id space
				if(isFileInMyIdSpace(hashFunction(myFiles.get(i)))==false)
				{
					filesCount++;
				}
			}
		 
			dos.writeInt(filesCount);
			
			//System.out.println("filescount is " + filesCount);
			
			String fileName = "";
			
			int n = 0;
		    byte[]buf = new byte[40920];
		    byte[] done = new byte[3];
			String str = "done";  //randomly anything
			done = str.getBytes();
			
			dos.flush();
		    for(int i=0; i< myFiles.size(); i++)
			{
				fileName = myFiles.get(i);
				
				//writing the files one by one
				if(isFileInMyIdSpace(hashFunction(fileName))== false)
				{
										
					dos.writeUTF(fileName);
					dos.flush();
					
				}
				
			}
		    
		   /* for(int i=0; i< myFiles.size(); i++)
			{
				fileName = myFiles.get(i);
				
				//writing the files one by one
				if(isInMyIdSpace(hashFunction(fileName))== false)
				{
					System.out.println("writing file " + i + " name is " + fileName);
					
					String filePath = "/home/stu4/s6/hr1652/DS/proj1/" + myIp + "/" + fileName;
					File sendFile = new File(filePath);
					
					FileInputStream fis = new FileInputStream(sendFile);
				    
					while((n =fis.read(buf)) != -1)
					{
				        dos.write(buf,0,n);
				        System.out.println(n);
				        dos.flush();

		            }
				    
				    dos.write(done,0,3);
				    dos.flush();
				
					
					String filePath = "/home/stu4/s6/hr1652/DS/proj1/" + myIp + "/" + fileName;
					File sendFile = new File(filePath);
					int fileSize = (int) sendFile.length();
					byte[] fileBytes = new byte[fileSize];
					bis = new BufferedInputStream(new FileInputStream(sendFile));
					bis.read(fileBytes, 0, fileBytes.length); 
					
									
					
					OutputStream os = newSocket.getOutputStream();
					
					os.write(fileSize);
					os.write(fileBytes, 0, fileBytes.length);
					
					os.flush();
					bis.close();  
					
					System.out.println(fileName + " sent");		
					
					
					//removing the file in myIdSpace
					//sendFile.delete();
					
				}
			}*/
			
			dos.close();
			
			//System.out.println("my id space now is " + myIdSpaceStart + " to " + myIdSpaceEnd);
			
			newSocket.close();
			//UpdateFilesList();
			
			
			//myPredecessor = newNodeIp;
			myPredecessor = newNodeIp;
			
			if(changeFlag==true)
				myFlag = false;
				
			changeFlag = false;
			//System.out.println(changeFlag + "  change and mine  " + myFlag);
			view();
		}
		
		else
		{
			int totalNodes = (int) (Math.pow(2, m) - 1);
			int halfofTotalNodes = totalNodes/2;
						
			//frwd to successor
			if(newNodeId >= myIdSpaceEnd
					&& newNodeId <= (myIdSpaceEnd+halfofTotalNodes)%totalNodes)
			{
				Socket newSocket = new Socket(mySuccessor, 9999);
				
				DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
				dos.writeUTF("join");
				dos.writeUTF(newNodeIp);
				newSocket.close();
				
				//System.out.println("Forwarded join request to my Successor" + mySuccessor);
			}
			
			else
			{
				Socket newSocket = new Socket(myPredecessor, 9999);
				
				DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
				dos.writeUTF("join");
				dos.writeUTF(newNodeIp);
				newSocket.close();
				
				//System.out.println("Forwarded join request to my Predecessor" + myPredecessor);
			}
			
		}
	
		
	}
	
	public void getFilesList() throws UnknownHostException
	{
		File folder = new File("/home/stu4/s6/hr1652/DS/proj1/" + getMyIp() + "/");
		File[] filesList = folder.listFiles();
				
		if(filesList.length > 0)
		{
			System.out.println("The files present are ...");
			
			for(int i=0; i<filesList.length; i++)
			{
				if(filesList[i].isFile())
				{
					System.out.println(filesList[i].getName());
				}
			}
		}
		
		else
			System.out.println("There are no files!");
		
	}
	
	public String getMyIp() throws UnknownHostException
	{
		String fullIp = ""+ InetAddress.getLocalHost();
		int cutOff = fullIp.indexOf("/");
		String ip = fullIp.substring(0, cutOff)+ ".cs.rit.edu";
		return ip;
	}
	
	public void insertKeyword(String fileName, String insertReqNodeId) throws IOException
	{	
		int fileHashValue = hashFunction(fileName);
		String filePath = "/home/stu4/s6/hr1652/DS/proj1/insert" + "/" + fileName;
		File sendFile = new File(filePath);
		int fileSize = (int) sendFile.length();
		byte[] fileBytes = new byte[fileSize];
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(sendFile));
		bis.read(fileBytes, 0, fileBytes.length);
		route1 = route1 + " --> " + myIp;
		//System.out.println("adding myip " + myIp);
		//System.out.println("now route is " + route1);
		//if file belongs to same node
		if(isFileInMyIdSpace(fileHashValue))
		{
			if(insertReqNodeId.equalsIgnoreCase(myIp))
			{
				//System.out.println("Storing the file locally");
				String newfilePath = "/home/stu4/s6/hr1652/DS/proj1/" + getMyIp() + "/" + fileName;
				FileOutputStream fos = new FileOutputStream(newfilePath);
				fos.write(fileBytes);
				fos.close(); 
				bis.close();
				System.out.println("The file " + fileName + " is stored in peer " + myIp);
				System.out.println("The route is direct");
			}
			
			else
			{
				Socket newSocket = new Socket(insertReqNodeId, 9999);
				//System.out.println("requesting initiator to send file " + fileName);
				DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
				dos.writeUTF("sendmeinsertfile");
				dos.writeUTF(fileName);
				dos.writeUTF(route1);
				dos.writeUTF(myIp);
				newSocket.close();
			}
			
 		}
		
		else
		{
			int totalNodes = (int) (Math.pow(2, m) - 1);
			int halfofTotalNodes = totalNodes/2;
						
			//frwd to successor
			if(fileHashValue > myIdSpaceEnd
					&& fileHashValue < (myIdSpaceEnd + halfofTotalNodes) % totalNodes)
			{				
				Socket newSocket = new Socket(mySuccessor, 9999);
				//System.out.println("Forwarding insert request to my Successor");
				DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
				
				dos.writeUTF("insert");
				dos.writeUTF(fileName);
				dos.writeUTF(route1);
				dos.writeUTF(insertReqNodeId);
				
				newSocket.close();
			}
			
			//frwd to predecessor
			else
			{
				Socket newSocket = new Socket(myPredecessor, 9999);
				//System.out.println("Forwarding insert request to my predecessor");
				DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());

				dos.writeUTF("insert");
				dos.writeUTF(fileName);
				dos.writeUTF(route1);
				dos.writeUTF(insertReqNodeId);
				
				newSocket.close();
			}
		}
		
	}
	
	public void insertHandler(Socket newSocket) throws IOException
	{
		
			InputStream is = newSocket.getInputStream();
			DataInputStream dis = new DataInputStream(is);
			
			String fileName = dis.readUTF();
			route1 = dis.readUTF();
			String insertReqNodeId = dis.readUTF();
			//System.out.println("file request received for file " + fileName);
			insertKeyword(fileName, insertReqNodeId);
	}
	
	public void insertRequest(String requestingId, String fileName) throws IOException
	{
		
		String filePath = "/home/stu4/s6/hr1652/DS/proj1/insert" + "/" + fileName;
		File sendFile = new File(filePath);
		int fileSize = (int) sendFile.length();
		byte[] fileBytes = new byte[fileSize];
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(sendFile));
		bis.read(fileBytes, 0, fileBytes.length);
		
		Socket newSocket = new Socket(requestingId, 9999);
		
		DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
		dos.writeUTF("insertResult");
		dos.writeUTF(fileName);
		dos.flush();
		OutputStream os = newSocket.getOutputStream();
		
		os.write(fileSize);
		os.write(fileBytes, 0, fileBytes.length);
		os.flush();
		//System.out.println("File sent to requesting node");		
		bis.close();
		newSocket.close();
		
		System.out.println("The file is stored in peer " + requestingId);
		System.out.println("The route is " + route1);
		route1 = "";
	}
	
	public void insertResult(String fileName, Socket sockt) throws IOException
	{
		File newFile = new File("/home/stu4/s6/hr1652/DS/proj1/" + myIp + "/" + fileName);
		
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(newFile));
		InputStream is = sockt.getInputStream();
		
		int fileSize = is.read();
		byte[] fileBytes = new byte[fileSize];
		int bytesSize = 0;
		
		while((bytesSize = is.read(fileBytes)) != -1)
			bos.write(fileBytes, 0, bytesSize);
		
		bos.close();
		
		route1 = "";
	}
	
		public void searchKeyword(String fileName, String searchReqNodeId) throws UnknownHostException, IOException
		{
			UpdateFilesList();
			int fileHashValue = hashFunction(fileName);
			route = route + " --> " + myIp;
			
			Boolean searchFlag = false;
			
			//if file belongs to same node
			if(isFileInMyIdSpace(fileHashValue))
			{
				//System.out.println("file should be in my id space");
				
				for(int i=0; i< myFiles.size() ; i++)
				{ 
					//System.out.println(myFiles.get(i));
					if(fileName.equalsIgnoreCase(myFiles.get(i)))
					{
						if(searchReqNodeId.equalsIgnoreCase(myIp))
						{
							System.out.println("\nSearch Success");
							System.out.println("The file " + fileName + " is stored in peer " + myIp);
							System.out.println("The route is direct");
						}
						
						else
						{
							//System.out.println("sending search result to requested node");
							Socket newSocket = new Socket(searchReqNodeId, 9999);
										
							DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
							dos.writeUTF("searchResult");
							dos.writeUTF(route);
							dos.writeUTF(myIp);
							newSocket.close();
						}
						
						searchFlag = true;
					}
				}
				
				if(searchFlag == false)
				{
					//System.out.println("Search Failure");
					Socket newSocket = new Socket(searchReqNodeId, 9999);
					
					DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
					dos.writeUTF("searchFailure");
					newSocket.close();
				}
	 		}
			
			else
			{
				int totalNodes = (int) (Math.pow(2, m) - 1);
				int halfofTotalNodes = totalNodes/2;
							
				//frwd to successor
				if(fileHashValue >= myIdSpaceEnd
						&& fileHashValue <= (myIdSpaceEnd+halfofTotalNodes)%totalNodes)
				{				
					//System.out.println("Forwarding search request to my Successor");
					Socket newSocket = new Socket(mySuccessor, 9999);
								
					DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
					dos.writeUTF("search");
					dos.writeUTF(fileName);
					dos.writeUTF(route);
					dos.writeUTF(searchReqNodeId);
					newSocket.close();
				}
				
				//frwd to predecessor
				else
				{
					//System.out.println("Forwarding search request to my Predecessor");
					Socket newSocket = new Socket(myPredecessor, 9999);
					
					DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
					dos.writeUTF("search");
					dos.writeUTF(fileName);
					dos.writeUTF(route);
					dos.writeUTF(searchReqNodeId);
					newSocket.close();
				}
			 }
			
			searchFlag = false;
			route = "";
		}
		
		public void searchHandler(Socket newSocket) throws IOException
		{
				InputStream is = newSocket.getInputStream();
				DataInputStream dis = new DataInputStream(is);
				
				String fileName = dis.readUTF();
				route = dis.readUTF();
				String searchReqNodeId = dis.readUTF();
				
				searchKeyword(fileName, searchReqNodeId);
		}
		
		public void searchResult(Socket newSocket) throws IOException
		{
			InputStream is = newSocket.getInputStream();
			DataInputStream dis = new DataInputStream(is);

			System.out.println("\n Search Success");
			route = dis.readUTF();
			String resultNodeId = dis.readUTF();
			
			System.out.println("The file is stored in peer " + resultNodeId);
			System.out.println("The route is " + route);
			
			route = "";
		}
		
	public void view() throws UnknownHostException
	{
		System.out.println();
		System.out.println("My ip is : " + getMyIp());
		System.out.println("My nodeId is : N" + myNodeId);
		System.out.println("My id Space is from " + myIdSpaceStart + " to " + myIdSpaceEnd);
		System.out.println("My predecessor is : " + myPredecessor);
		System.out.println("My successor is : " + mySuccessor);
		PrintFilesList();
		if(myFlag == true)
			System.out.println("I have 0th id space");
	}
	
	public void joinKeyword() throws IOException
	{
		if(joinFlag == false)
		{
			Socket newSocket = new Socket(bsServerIp, 9999);
			
			DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
			
			dos.writeUTF("join");
			dos.writeUTF(myIp);		
			//System.out.println("Requested to join");
			
			newSocket.close();
		}
		
		else
			System.out.println("Already joined");
	}
	
	public void joinResult(Socket newSocket) throws IOException
	{
		InputStream is = newSocket.getInputStream();
		DataInputStream dis = new DataInputStream(is);
		
		myFlag = dis.readBoolean();
		
		myIdSpaceStart = dis.read();
		myIdSpaceEnd = dis.read();
		
		myPredecessor = dis.readUTF();
		mySuccessor = dis.readUTF();
		//System.out.println("My id Space is from " + myIdSpaceStart + " to " + myIdSpaceEnd);
		
		int filesCount = dis.readInt();
			        
	    String[] gotFiles = new String[filesCount];
		for(int i=0; i<filesCount; i++)
		{
			//System.out.println("waiting for file");
			gotFiles[i] = dis.readUTF();
			myIp = getMyIp();
			File gotFile = new File("/home/stu4/s6/hr1652/DS/proj1/" + myIp + "/" + gotFiles[i]);
			//System.out.println(gotFiles[i] + " received");
		}
			
		/*for(int i=0; i<filesCount; i++)
		{
			System.out.println("filename got is " + gotFiles[i]); 
			
			myIp = getMyIp();
			File gotFile = new File("/home/stu4/s6/hr1652/DS/proj1/" + myIp + "/" + gotFiles[i]);
			FileOutputStream fos = new FileOutputStream(gotFile);
			
			while((n = dis.read(buf)) != -1 && n!=3 )
			{
		        fos.write(buf,0,n);
		        fos.flush();
		        
			}
		        fos.close();
			
		     System.out.println(gotFiles[i] + " received");
			try 
			{
				System.out.println("waiting for file");
				fileName = dis.readUTF();
			} 
			
			catch (IOException e) 
			{
				e.printStackTrace();
			}
			
			System.out.println("filename got is " + fileName); 
			myIp = getMyIp();
			File gotFile = new File("/home/stu4/s6/hr1652/DS/proj1/" + myIp + "/" + fileName);
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(gotFile));
			is = newSocket.getInputStream();
			int fileSize = is.read();
			byte[] fileBytes = new byte[fileSize];
			int bytesSize = 0;
			
			while((bytesSize = is.read(fileBytes)) != -1)
				bos.write(fileBytes, 0, bytesSize);
			
			bos.close();
			System.out.println(fileName + " received");
		}*/
		
		joinFlag = true;
		updatePredecessor();
		newSocket.close();
		
		view();
	}
	
	public void updatePredecessor() throws IOException
	{
		int port = 9999;
			
		Socket newSocket = new Socket(myPredecessor, port);
		
		DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
		dos.writeUTF("updateyoursuccessor");
		dos.writeUTF(myIp);
		newSocket.close();
		//System.out.println("Updated my Predecessor");
	}
		
	public void leave() 
	{
		try
		{
			//inform predecessor
			int port = 9999;
			
			/*if(myPredecessor.equalsIgnoreCase(bsServerIp))
			{
				port = 9999;
			}*/
			
			Socket newSocket = new Socket(myPredecessor, port);
			//System.out.println("updating my predecessor : " + myPredecessor);
			DataOutputStream dos = new DataOutputStream(newSocket.getOutputStream());
			dos.writeUTF("updateyoursuccessor");
			dos.writeUTF(mySuccessor);
			newSocket.close();
			System.out.println("Updated my predecessor");
			
			int port1 = 9999;
			//inform successor
			/*if(mySuccessor.equalsIgnoreCase(bsServerIp))
			{
				port1 = 9999;
			}*/
			
			Socket newSocket1 = new Socket(mySuccessor, port1);
			//System.out.println("updating my successor : " + mySuccessor);
			DataOutputStream dos1 = new DataOutputStream(newSocket1.getOutputStream());
			dos1.writeUTF("updateyourpredecessor");
			dos1.writeUTF(myPredecessor);
			newSocket1.close();
			System.out.println("Updated my Successor");
			
			//sending all files to my successor
			Socket newSocket2 = new Socket(mySuccessor, port);
			
			/*DataOutputStream dos2 = new DataOutputStream(newSocket2.getOutputStream());
			dos2.writeUTF("getfilesbcosleave");
			dos2.writeInt(myIdSpaceStart);
			
			int filesCount = myFiles.size();
			
			dos2.writeInt(filesCount);
			System.out.println("filescount is " + filesCount);
			
			for(int i=0; i< myFiles.size(); i++)
			{
				System.out.println("seding file " + myFiles.get(i));
				dos.writeUTF(myFiles.get(i));
				dos.flush();
				//removing the file in myIdSpace
				//sendFile.delete();
			} 
			
			newSocket2.close();*/
			//System.out.println("sent all files to my Successor");
		}
		
		catch (IOException e1) 
		{
			System.out.println("Leave Failure");
			e1.printStackTrace();
		}
	}
	
	public void leaveHandler(Socket newSocket) throws IOException
	{
		InputStream is = newSocket.getInputStream();
		DataInputStream dis = new DataInputStream(is);
		myIdSpaceStart = dis.read();
				
		int filesCount = dis.readInt();
			        
		System.out.println("files count got is " + filesCount);
	    String[] gotFiles = new String[filesCount];
	    
		for(int i=0; i<filesCount; i++)
		{
			//System.out.println("waiting for file");
			gotFiles[i] = dis.readUTF();
			myIp = getMyIp();
			File gotFile = new File("/home/stu4/s6/hr1652/DS/proj1/" + myIp + "/" + gotFiles[i]);
			System.out.println(gotFiles[i] + " received");
		}
	}
	
	public void run()
	{
		Scanner sc = new Scanner(System.in);
		
		
		while(true)
		{
			System.out.println("\n Enter command :");
			System.out.println(" (insert, search, view, join, leave, e to exit) \n");
			String input = sc.next();
			switch (input) {
			case "insert" :
				try 
				{
					String fileName = sc.next();	
					insertKeyword(fileName, myIp);
				} 
				
				catch (IOException e) 
				{
					e.printStackTrace();
				}
				break;

			case "search" :
				try 
				{
					String fileName = sc.next();
					
					searchKeyword(fileName, myIp);
				} 
				
				catch (IOException e) 
				{
					e.printStackTrace();
				}
				
				break;
				
			case "view" :
				try {
					view();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
				break;
				
			case "join" : 
				try {
					joinKeyword();
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;
				
			case "leave" :
				
				leave();
				
				break;
				
			case "e" :
				sc.close();
				System.exit(1);
				
			default:
				System.out.println("Wrong command");
				break;
			}
			
			try 
			{
				Thread.sleep(2000);
			}
			
			catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
				
		
	}

	public void initialize() throws UnknownHostException
	{
		myIp = getMyIp();
		myNodeId = hashFunction(myIp);
		mySuccessor = "";
		myPredecessor = "";
		myIdSpaceStart = -1 ;
		myIdSpaceEnd = myNodeId;
		m = 5;
		myFiles = new ArrayList<String>();
		route = "";
		bsServerIp = "rhea.cs.rit.edu";
		myFlag = false;
		joinFlag = false;
		changeFlag = false;
		route1 = "";

	}
	
	public static void main(String args[]) throws IOException
	{
		Node obj1 = new Node();		
		obj1.initialize();
		obj1.start();
		
		ListenAsServer obj2 = new ListenAsServer();
		obj2.start();
		
	}
	
}


class ListenAsServer extends Thread
{
	Node objj = new Node();
	
	
	
	public void insertRequest(Socket sockt, Node objj2) throws IOException
	{
		InputStream is = sockt.getInputStream();
		
		DataInputStream dis = new DataInputStream(is);
		String fileName = dis.readUTF();
		System.out.println(fileName + " aaaa ");
				
		File gotFile = new File("/home/stu4/s6/hr1652/DS/proj1/" + objj2.getMyIp() + "/" + fileName);
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(gotFile));
		
		int fileSize = is.read();
		byte[] fileBytes = new byte[fileSize];
		int bytesSize = 0;
		
		while((bytesSize = is.read(fileBytes)) != -1)
			bos.write(fileBytes, 0, bytesSize);
		
		bos.close();
		System.out.println("File received");
	}
	
	public void run()
	{
		ServerSocket servSock = null;
		try {
			servSock = new ServerSocket(9999);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		while(true)
		{
			Socket sockt = null;
			String input = "";
			InputStream is;
			DataInputStream dis = null;
			try 
			{
				sockt = servSock.accept();
				//System.out.println("Server ready");
				is = sockt.getInputStream();
				dis = new DataInputStream(is);
				
				input = dis.readUTF();
			} 
			
			catch (IOException e1) 
			{
				e1.printStackTrace();
			} 
			
			switch (input) 
			{
			
			case "insert" :
				
				try 
				{
					objj.insertHandler(sockt);
				} 
				
				catch (IOException e) 
				{
					e.printStackTrace();
				}
				break;
				
			case "sendmeinsertfile" :
				
				try 
				{
					String fileName = dis.readUTF();
					objj.route1 = dis.readUTF();
					String requestingId = dis.readUTF();
					
					objj.insertRequest(requestingId, fileName);
				} 
				
				catch (IOException e1) 
				{
					e1.printStackTrace();
				}				
				break;
				
			case "insertResult" :
				
				try 
				{
					String fileName = dis.readUTF();
					objj.insertResult(fileName, sockt);
				} 
				
				catch (IOException e1) 
				{
					e1.printStackTrace();
				}				
				break;

			case "search" :
				try 
				{
					
					objj.searchHandler(sockt);
				} 
				
				catch (IOException e) 
				{
					e.printStackTrace();
				}
				
				break;
							
			case "searchResult" :
				
				try 
				{
					objj.searchResult(sockt);
				} 
				
				catch (IOException e) 
				{
					e.printStackTrace();
				}
				break;
				
				
			case "searchFailure" :
				
				System.out.println("Search Failure");
				break;
				
			case "joinResult" : 
				try 
				{
					objj.joinResult(sockt);
				} 
				
				catch (IOException e) 
				{
					e.printStackTrace();
				}
				
				break;
							
			case "join" : 
				
				try 
				{
					String newNodeIp = dis.readUTF();
					objj.joinHandler(newNodeIp);
					
				} 
				
				catch (IOException e) 
				{
					e.printStackTrace();
				}
				
				break;
				
			case "updateyourpredecessor" :
			
				try 
				{
					//System.out.println("Request received for updating my predecessor");
					Node.myPredecessor = dis.readUTF();
				} 
				
				catch (IOException e) 
				{
					e.printStackTrace();
				}
				
				break;
				
			case "updateyoursuccessor" :
				
				try 
				{
					//System.out.println("Request received for updating my successor");
					Node.mySuccessor = dis.readUTF();
				} 
				
				catch (IOException e) 
				{
					e.printStackTrace();
				}
				
				break;
				
			case "getfilesbcosleave" : 
				
				try 
				{
					objj.leaveHandler(sockt);
				} 
				
				catch (IOException e) 
				{
					e.printStackTrace();
				}
				break;
				
			default:
				System.out.println("Wrong command received");
				break;
			}
			
		}		
		                                        
	}
	
}

