import java.util.Scanner;

public class hashh {

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
		System.out.println(hash);
		
		return hash;
		
		
	}
	
	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		hashh obj = new hashh();
		
		while(true)
			{
				String fName = sc.nextLine();
				obj.hashFunction(fName);
			}
		
	}

}
