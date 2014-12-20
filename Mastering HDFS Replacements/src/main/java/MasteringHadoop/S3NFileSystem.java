package MasteringHadoop;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;

/**
 * Created by sandeepkaranth on 29/07/14.
 */
public class S3NFileSystem extends FileSystem {

    private URI uri;
    private AmazonS3Client s3Client;
    private Configuration configuration;
    private String bucket;

    public S3NFileSystem(){
        super();
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {

        super.initialize(name, conf);
        this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());

        String accessKey = conf.get("fs.s3mh.access.key");
        String secretKey = conf.get("fs.s3mh.secret.key");

        System.out.println("Access Key: "  + accessKey);

        s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));

        this.bucket = name.getHost();

        if(!s3Client.doesBucketExist(this.bucket)){
            throw new IOException("Bucket " + this.bucket + " does not exist!");
        }

        this.configuration = conf;


    }

    @Override
    public String getScheme() {
        return "s3mh";

    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
       FileStatus fs = getFileStatus(path);
       return new FSDataInputStream(new S3NFsInputStream(this.s3Client, this.configuration, this.bucket, pathToKey(path), fs.getLen()));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i2, long l, Progressable progressable) throws IOException {
        String key = pathToKey(path);
       return new FSDataOutputStream(new S3NFsOutputStream(this.s3Client, this.configuration, this.bucket, key), null);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new IOException("Append functionality is not supported");
    }

    @Override
    public boolean rename(Path path, Path path2) throws IOException {
        throw new IOException("Rename is copy followed by delete");
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {

        FileStatus fs = getFileStatus(path);

        if(b){
            throw new PathIOException("Recursive delete is not supported");
        }

        if(!fs.isDirectory()){
            s3Client.deleteObject(this.bucket, pathToKey(path));
        }

        return false;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {

        ArrayList<FileStatus> returnList = new ArrayList<>();
        String key = pathToKey(path);
        FileStatus fs = getFileStatus(path);


        if(fs.isDirectory()){

            if(!key.isEmpty()){
                key = key + "/";
            }

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest(this.bucket, key, null, "/", 1000);
            ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);

            for(S3ObjectSummary summary : objectListing.getObjectSummaries()){

                FileStatus fileStatus;
                if(isADirectory(summary.getKey(), summary.getSize())){
                    fileStatus = new FileStatus(summary.getSize(), true, 1, 0, 0, new Path("/" + key));
                }
                else{
                    fileStatus = new FileStatus(summary.getSize(), false, 1, 0, 0, new Path("/" + key));
                }

                returnList.add(fileStatus);

            }

        }
        else{
            returnList.add(fs);
        }

        return returnList.toArray(new FileStatus[returnList.size()]);
    }

    @Override
    public void setWorkingDirectory(Path path) {

    }

    @Override
    public Path getWorkingDirectory() {
        return null;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {

        String key = pathToKey(path);

        System.out.println("Key : " + key);
        System.out.println("Bucket : " + this.bucket)  ;

        if(key.isEmpty()){
            throw new IOException("File not found.");
        }

        ObjectMetadata objectMetadata = s3Client.getObjectMetadata(this.bucket, key);

        if(isADirectory(key, objectMetadata.getContentLength())){
            return new FileStatus(0, true, 1, 0, 0, path);
        }


        return new FileStatus(0, false, 1, 0, objectMetadata.getLastModified().getTime(), path);
    }


    private String pathToKey(Path path) {
        return path.toUri().getPath().substring(1);
    }

    private boolean isADirectory(String name, long size) {
        return !name.isEmpty()
               && name.charAt(name.length() - 1) == '/'
               && size == 0L;
    }

    private class S3NFsInputStream extends FSInputStream{

        private AmazonS3Client s3Client;
        private Configuration configuration;
        private String bucket;
        private String key;
        private long length;

        private S3ObjectInputStream s3ObjectInputStream;
        private S3Object s3Object;
        private long position;

        public S3NFsInputStream(AmazonS3Client s3, Configuration conf, String bucket, String key, long length) {
            super();

            this.s3Client = s3;
            this.configuration = conf;
            this.bucket = bucket;
            this.key = key;
            this.length = length;

            this.s3Object = null;

        }

        private void openObject(){

            if(s3Object == null){
                openS3Stream(0);
            }

        }

        private void openS3Stream(long position){

            if(s3ObjectInputStream != null){
                s3ObjectInputStream.abort();
            }

            GetObjectRequest objectRequest = new GetObjectRequest(this.bucket, this.key);
            objectRequest.setRange(position, length - 1);
            this.s3Object = this.s3Client.getObject(objectRequest);
            this.s3ObjectInputStream = this.s3Object.getObjectContent();

            this.position = position;

        }



        @Override
        public int read() throws IOException {

            openObject();
            int readByte = this.s3ObjectInputStream.read();

            if(readByte >= 0){
                this.position++;
            }

            return readByte;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {

            openObject();
            int readByte = this.s3ObjectInputStream.read(b, off, len);

            if(readByte >= 0){
                this.position+=readByte;

            }
            return readByte;
        }

        @Override
        public void close() throws IOException {
            super.close();

            if(s3Object != null){
                s3Object.close();
            }
        }


        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void seek(long l) throws IOException {

            if(this.position == l){
                return;
            }
            openS3Stream(l);

        }

        @Override
        public long getPos() throws IOException {
            return this.position;
        }

        @Override
        public boolean seekToNewSource(long l) throws IOException {
            return false;
        }


    }


    private class S3NFsOutputStream extends OutputStream{

        private OutputStream localFileStream;
        private AmazonS3Client s3Client;
        private LocalDirAllocator localDirAllocator;
        private Configuration configuration;
        private File backingFile;
        private BufferedOutputStream bufferedOutputStream;
        private String bucket;
        private String key;


        public S3NFsOutputStream(AmazonS3Client s3, Configuration conf, String bucket, String key) throws IOException{
            super();
            this.s3Client = s3;
            this.configuration = conf;
            this.localDirAllocator = new LocalDirAllocator("${hadoop.tmp.dir}/s3mh");

            this.backingFile = localDirAllocator.createTmpFileForWrite("temp", LocalDirAllocator.SIZE_UNKNOWN, conf);
            this.bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(this.backingFile));
            this.bucket = bucket;
            this.key = key;
        }


        @Override
        public void write(int b) throws IOException {
            this.bufferedOutputStream.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            this.bufferedOutputStream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            this.bufferedOutputStream.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            if(this.bufferedOutputStream != null){
                this.bufferedOutputStream.flush();
            }
        }

        @Override
        public void close() throws IOException {

            if(this.bufferedOutputStream != null){
                this.bufferedOutputStream.close();
            }


            try {
                PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, backingFile);
                putObjectRequest.setCannedAcl(CannedAccessControlList.Private);

                s3Client.putObject(putObjectRequest);
            }
            catch(AmazonServiceException ase){
                ase.printStackTrace();
            }

        }
    }

}
