package poc.hadoop;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFileOps {

	public static void main(String[] args) throws IOException, URISyntaxException {

		String hdfsPath = "hdfs://server1.bigdata.com:9000/"; // specifying the namenode hdfs path
		Configuration conf = new Configuration(); // provides access to configuration
		FileSystem fs = FileSystem.get(new URI(hdfsPath), conf);

		// Creating instance of the class HDFSFileOps
		HDFSFileOps fileOps = new HDFSFileOps();
		
		System.out.println("Available HDFS Operations:");
		System.out.println(" 1.List Files\n 2.Create Directory\n 3.Create Empty File\n 4.Copy File to Local");
		System.out.println(" 5.Upload File to HDFS\n 6.Read Url source and write to hdfs file\n 7.Read File from HDFS\n 8.Write to HDFS from Console");
		System.out.println("Choose your option from 1 to 8:");
		Scanner scan = new Scanner(System.in);
		int i = scan.nextInt();
		
		switch(i) {
		case 1:
			fileOps.listFiles(fs,hdfsPath);
			break;
		case 2:
			fileOps.createDirectory(fs);
			break;
		case 3:
			fileOps.createFile(fs);
			break;
		case 4:
			fileOps.copyFile(fs);
			break;
		case 5:
			fileOps.uploadFile(fs);
			break;
		case 6:
			fileOps.urlToHDFS(fs);
			break;
		case 7:
			fileOps.readFromHDFS(fs);
			break;
		case 8:
			fileOps.writeFromConsoleToHDFS(fs);
			break;
		default:
			System.out.println("Valid option not selected");
		}
		
		fs.close();
		scan.close();
	}


	public void listFiles(FileSystem fs, String hdfsPath) throws FileNotFoundException, IOException {
		// listStatus method- list the immediate entries in the provided path
		FileStatus[] fileStatus = fs.listStatus(new Path(hdfsPath));
		for (FileStatus status : fileStatus) {
			System.out.println(status.getPath());
			// call recursively to get all the files if the current path is of directory
			if (status.isDirectory()) {
				listFiles(fs, status.getPath().toString());	
			}
		}
	}

	// Create a Directory using mkdirs method. mkdirs method return true on success
	// and false when operation failed

	public void createDirectory(FileSystem fs) throws IOException, URISyntaxException {
		Scanner scan = new Scanner(System.in);
		System.out.println("Enter the name of directory you want to create (example mydirectory or user/mydirectory:");
		String newDirectoryName = scan.nextLine();
		String newDirectoryPath = newDirectoryName;
		Path path = new Path(newDirectoryPath);
		Boolean opsSuccess = fs.mkdirs(path);
		if (opsSuccess) {
			System.out.println(" Directory successfully created. \n Directory Name :" + newDirectoryName + " \n Path :"+ newDirectoryPath);
		} else {
			System.out.println(" Operation failed. Check if directory already exist");
		}
		scan.close();
	}
	

	// create a new file. createNewFile method return false if file already exist or operation failed.

	public void createFile(FileSystem fs) throws IOException, URISyntaxException {
		Scanner scan = new Scanner(System.in);
		System.out.println("Enter the filename:");
		String fileName = scan.nextLine();
		System.out.println("Enetr the HDFS directory path where you want to create new file example (user/documents/ or user/):");
		String directoryPath =scan.nextLine();
		String filePath = directoryPath+fileName;
		Path path = new Path(filePath);
		Boolean opsSuccess = fs.createNewFile(path);
		if (opsSuccess) {
			System.out.println(" New file successfull created.\n File Name:" + fileName);
		} else {
			System.out.println(" Operation failed. Check if file already exist");
		}
		scan.close();
	}

	// Copy File to Local

	public void copyFile(FileSystem fs) throws IOException {
		Scanner scan = new Scanner(System.in);
		System.out.println("Enter the local directory path:");
		String fileLocalLocation = scan.nextLine();
		System.out.println("Enetr the HDFS file path from where you want to copy file example (user/test-file-01 or /user/test-file-01):");
		String directoryPath =scan.nextLine();
		Path localPath = new Path(fileLocalLocation);
		Path path = new Path(directoryPath);
		fs.copyToLocalFile(path, localPath);
		scan.close();
	}

	// Copy File to HDFS
	public void uploadFile(FileSystem fs) throws IOException {

		Scanner scan = new Scanner(System.in);
		System.out.println("Enter the local directory path:");
		String fileLocalLocation = scan.nextLine();
		System.out.println("Enetr the HDFS file path to create a new file \n Example:(user/test-file-01 or /user/test-file-01 - here test-file-01 is new file name):");
		String hdfsFilePath =scan.nextLine();
		Path localPath = new Path(fileLocalLocation);
		Path path = new Path(hdfsFilePath);
		fs.delete(path, true);
		fs.copyFromLocalFile(true, true, localPath, path);
		scan.close();

	}


	// Read source data from a given url and write the data in HDFS
	public void urlToHDFS(FileSystem fs) throws IOException, URISyntaxException {
		System.setProperty("java.net.preferIPv4Stack", "true");
		Scanner scan = new Scanner(System.in);
		System.out.println("Enter the url from which you want to copy source:");
		String urlString = scan.nextLine();
		System.out.println("Enetr the HDFS file path to copy src \n Example:(user/test-file-01 or /user/test-file-01 - here test-file-01 is new file name):");
		String hdfsFilePath =scan.nextLine();
		Path path = new Path(hdfsFilePath);
		FSDataOutputStream hdfsFileWriter = fs.append(path);

		HttpURLConnection httpUrlConn = (HttpURLConnection) new URL(urlString).openConnection();
		httpUrlConn.setRequestMethod("GET");
		httpUrlConn.setConnectTimeout(30000);
		httpUrlConn.setReadTimeout(30000);
		httpUrlConn.getResponseMessage();

		InputStream in = httpUrlConn.getInputStream();
		int ch;
		while ((ch = in.read()) != -1) {
			hdfsFileWriter.writeByte(ch);
		}
		hdfsFileWriter.close();
		
	}

	// HDFS read from HDFS and write to console
	public void readFromHDFS(FileSystem fs) throws IOException, URISyntaxException {
		Scanner scan = new Scanner(System.in);
		System.out.println("Enetr the HDFS file path to read\n Example:(user/test-file-01 or /user/test-file-01 - here test-file-01 is new file name):");
		String hdfsFilePath =scan.nextLine();
		Path path = new Path(hdfsFilePath);
		FSDataInputStream in = fs.open(path);
		System.out.println(fs.getDefaultBlockSize(path));
		String ch;
		
		while ((ch = in.readUTF()) != null) {
			System.out.println(ch);
		}
		
		in.close();
		scan.close();
	}
	
	// Reading the entry from console and writing it in the database
	public void writeFromConsoleToHDFS(FileSystem fs) throws IOException, URISyntaxException {
		Scanner scan = new Scanner(System.in);
		System.out.println("Enter the HDFS file path to write\n Example:(user/test-file-01 or /user/test-file-01 - here test-file-01 is new file name):");
		String hdfsFilePath =scan.nextLine();
		Path path = new Path(hdfsFilePath);
		FSDataOutputStream hdfsWriter = fs.append(path);
		
		boolean ptr = true;
		String str;
		while(ptr) {
			System.out.println("enter the line (last line should be 'end' to quit):");
			if((str=scan.nextLine()).equals("end")) {
				ptr=false;
			} else {
				
				hdfsWriter.writeBytes("\n"+str);
				
			}
		}
		hdfsWriter.close();
		scan.close();
	}
	
	
}
