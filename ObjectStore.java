import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.Stack;


import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ObjectStore {
	public final String dataParentPath="/Users/anu/Documents/objectstore/data/";
	public final String linkParentPath="/Users/anu/Documents/objectstore/links/";
	public final String stateFilePath="/Users/anu/Documents/objectstore/state";
	public final int sizeofRC=4;
	
	public String readLinkFile(int oid) throws Exception{
		DataInputStream fis=new DataInputStream(new FileInputStream(linkParentPath+oid) );
		System.out.println(linkParentPath+oid);
		byte[] buffer=new byte[4];

		String hashcode="";
		while((fis.read(buffer))!=-1){
			hashcode+=buffer.toString();
		}
		fis.close();
		return hashcode;

	}

	public int update_rc(String datafile,int offset) throws Exception{
		int rc=-1;
		RandomAccessFile f=new RandomAccessFile(dataParentPath+datafile,"rw");
		rc=f.readInt();
		rc+=offset;
		f.seek(0);
		f.writeInt(rc);
		f.close();
		return rc;
	}

	public void addLinkFile(String hashcode,String linkfile){
		try{
			DataOutputStream fos=new DataOutputStream(new FileOutputStream(linkfile));
			byte[] buffer=hashcode.getBytes();
			fos.write(buffer);
			fos.close();
		}
		catch(Exception e){

		}
	}

	public class ObjStore{
		int obid;
		Stack<Integer> freeObid=new Stack<Integer>();
		Lock lock;

		ObjStore(){
			loadState();
			File dataf=new File(dataParentPath);
			if(!dataf.exists()){
				dataf.mkdirs();
			}
			File linkf=new File(linkParentPath);
			if(!linkf.exists()){
				linkf.mkdirs();
			}
			lock=new ReentrantLock();
		}


		public boolean loadState()  {
			obid=1;
			DataInputStream fis;
			try{
				fis=new DataInputStream(new FileInputStream(stateFilePath) );
			}
			catch(Exception e){
				return false;
			}
			try{
				obid=fis.readInt();
			}
			catch(Exception e){
				
				System.out.println("Object id was not read");
				try{
				fis.close();
				}
				catch(Exception exp){
					
				}
				return false;
			}
			while(true){
				try{
					freeObid.add(fis.readInt());
				}
				catch(EOFException exp){
					break;
				}
				catch(Exception e){
					
				}
				finally{
					try {
						fis.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
			
			return true;

		}

		public void storeState() throws Exception{
			int n=0;
			DataOutputStream fos=new DataOutputStream(new FileOutputStream(stateFilePath) );
			try{
				fos.writeInt(obid);
			}
			catch(IOException e){
				System.out.println("Object id was not correctly written");
			}
			while(n<freeObid.size()){
				try{
					fos.writeInt(freeObid.get(n));
					n++;
				}
				catch(IOException e){
					System.out.println("Free Object ids was not read");
				}
			}
			fos.close();

		}
		public String generateHashCode(byte[] data) throws Exception{
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			byte[] hash=md.digest(data);
			String hashcode=hash.toString();
			return hashcode;
		}
		public int put(byte[] data) throws Exception{
			String filename=generateHashCode(data);
			lock.lock();
			if( freeObid.empty()){
				obid+=1;
			}
			else{
				obid=freeObid.pop();
			}
			String objfile=linkParentPath+obid;
			File datafile=new File(dataParentPath+filename);
			if(!datafile.exists()){
				lock.unlock();
				File tmpfile=new File(dataParentPath+obid);
				RandomAccessFile f=new RandomAccessFile(tmpfile, "rw") ;
				f.writeInt(1);
				f.write(data);
				lock.lock();
				if(!datafile.exists()){
					tmpfile.renameTo(datafile);
				}
				else{
					update_rc(filename,1);
					tmpfile.delete();
				}
				f.close();
			}
			addLinkFile(filename, objfile);

			lock.unlock();
			return obid;
		}

		public String get(int objid) throws Exception{
			String hashcode = null;
			File objFile=new File(linkParentPath+objid);
			lock.lock();
			if(objFile.exists()){
				hashcode=readLinkFile(objid);
			}
			lock.unlock();
			String data="";
			File dataFile=new File(dataParentPath+hashcode);
			System.out.println(dataParentPath+hashcode);
			if(dataFile.exists()){
				DataInputStream fis=new DataInputStream(new FileInputStream(dataFile) );
				byte[] buffer=new byte[4];
				fis.read(buffer);
				while((fis.read(buffer))!=-1){
					System.out.println(buffer);
					data+=buffer.toString();
				}
				fis.close();
			}

			return data;
		}

		public void delete(int objid) throws Exception{
			String hashcode=null;
			File objFile=new File(linkParentPath+objid);
			lock.lock();
			if(objFile.exists()){
				hashcode=readLinkFile(objid);
				File dataFile=new File(dataParentPath+hashcode);
				if(dataFile.exists()){
					int rc=update_rc(dataParentPath+hashcode, -1);
					if(rc==0){
						dataFile.delete();
					}
				}
				objFile.delete();
				freeObid.push(objid);
				lock.unlock();
			}
		}
	}
	public static void main(String args[]) throws Exception{
		byte[] data="This is sample data".getBytes();
		ObjectStore obj=new ObjectStore();
		ObjStore ob = obj.new ObjStore();
		for(int i=0;i<100;i++){
		ob.put(data);
		}
		ob.delete(5);
		ob.delete(50);
		ob.put("Hello this is good".getBytes());
		System.out.println(ob.get(5));
		ob.storeState();
	}
}
