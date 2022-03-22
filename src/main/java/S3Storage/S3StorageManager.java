package S3Storage;

import WindwardModels.*;
import WindwardRepository.RepositoryStatus;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class S3StorageManager {

    private static AmazonDynamoDB Client;
    protected static DynamoDB DynamoDb;
    private static Table RepositoryTable;
    private AmazonS3Client S3Client;
    private DynamoDBMapper DynamoMapper;

    private static String BucketName;
    private static String DocumentsFolder;
    private static String TemplatesFolder;

    private static final Logger Log = LogManager.getLogger(S3StorageManager.class);


    public S3StorageManager(BasicAWSCredentials aWSCredentials,  AmazonS3Client s3Client, String bucketName)
    {
        createClient(aWSCredentials);
        BucketName = bucketName;
        DocumentsFolder = bucketName + "/Documents";
        TemplatesFolder = bucketName + "/Templates";
        S3Client = s3Client;
    }

    public boolean AddRequest(JobRequestData requestData)
    {
        JobInfoEntity entity = JobInfoEntity.FromJobRequestData(requestData);
        entity.Status = (int)RepositoryStatus.JOB_STATUS.Pending.getValue();

        try
        {
            DynamoMapper.save(entity);
            Log.debug("[S3StorageManager AddRequest] Added template ["+ requestData.Template.getGuid() +"] to blob storage");


            ObjectMapper mapper = new ObjectMapper();
            String test = mapper.writeValueAsString(requestData.Template);

            InputStream stream = new ByteArrayInputStream(test.getBytes
                    (Charset.forName("UTF-8")));
            PutObjectRequest objectRequest = new PutObjectRequest(TemplatesFolder, entity.Guid, stream, null);

            S3Client.putObject(objectRequest);

            return true;
        }
        catch (Exception ex)
        {
            Log.error("[S3StorageManager AddRequest] Error updating request: ", ex);
            return false;
        }
    }


    public boolean UpdateRequest(String guid, RepositoryStatus.JOB_STATUS newStatus)
    {
        JobInfoEntity entity = getRequestInfo(guid);
        entity.Status = (int)newStatus.getValue();
        boolean success = false;
        try
        {
            DynamoMapper.save(entity);
            Log.info("[S3StorageManager] Updated request: " + guid);
            success = true;
            return success;
        }
        catch (Exception ex)
        {
            Log.error("[S3StorageManager] Error updating request: ", ex);
            return success;
        }

    }

    public <T> boolean completeRequest(String guid, T generatedEntity) throws IOException {
        JobInfoEntity entity = getRequestInfo(guid);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(generatedEntity);

        InputStream stream = new ByteArrayInputStream(json.getBytes
                (Charset.forName("UTF-8")));

        if(!(generatedEntity instanceof ServiceError))
        {
            entity.setStatus(RepositoryStatus.JOB_STATUS.Complete.getValue());
        }

        PutObjectRequest req = new PutObjectRequest(DocumentsFolder, guid, stream, new ObjectMetadata());
        try {
            S3Client.putObject(req);
            DynamoMapper.save(entity);
            return true;
        }
        catch (AmazonServiceException ex)
        {
            Log.error("[S3StorageManager completeReques threw an error when trying to put object in S3: "+ex);
            return false;
        }

    }

    public boolean deleteRequest(String guid)
    {
        try{
            Map<String, AttributeValue> tmp = null;
            tmp.put("Guid", new AttributeValue(guid));
            
            DeleteItemRequest req = new DeleteItemRequest(RepositoryTable.getTableName(), tmp);
            Client.deleteItem(req);

            S3Client.deleteObject(DocumentsFolder, guid);
            S3Client.deleteObject(TemplatesFolder, guid);

            return true;
        }
        catch (Exception ex)
        {
            Log.error("[S3StorageManager deleteRequest threw an error when trying to delete object in dynamo table: "+ex);
            return false;
        }
    }

    public boolean revertGeneratingJobsPending()
    {
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        scanExpression.addFilterCondition("Status", new Condition().withComparisonOperator(ComparisonOperator.EQ).
                withAttributeValueList(new AttributeValue().withN(String.valueOf(RepositoryStatus.JOB_STATUS.Generating.getValue()))));

        try {
            List<JobInfoEntity> tmp = DynamoMapper.scan(JobInfoEntity.class, scanExpression);
            for ( JobInfoEntity job : tmp)
            {
                UpdateRequest(job.Guid, RepositoryStatus.JOB_STATUS.Pending);
            }
            return true;
        }
        catch (Exception ex)
        {
            Log.error("[S3StorageManager revertGeneratingJobsPending() threw an error when trying to revert generating jobs: "+ex);
            return false;
        }
    }

    public void deleteOldRequests(LocalDateTime cutoff)
    {
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        scanExpression.addFilterCondition("CreationDate", new Condition().withComparisonOperator(ComparisonOperator.LE).
                withAttributeValueList(new AttributeValue().withN(String.valueOf(cutoff))));

        try{
            List<JobInfoEntity> tmp = DynamoMapper.scan(JobInfoEntity.class, scanExpression);
            for ( JobInfoEntity job : tmp)
            {
                deleteRequest(job.Guid);
            }
        }
        catch (Exception ex)
        {
            Log.error("[S3StorageManager] deleteOldRequests() threw an error when trying to delete old jobs: "+ex);
        }
    }

    public JobRequestData getOldestJobAndGenerate()
    {
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        scanExpression.addFilterCondition("Status", new Condition().withComparisonOperator(ComparisonOperator.EQ).
                withAttributeValueList(new AttributeValue().withN(String.valueOf(RepositoryStatus.JOB_STATUS.Pending.getValue()))));

        boolean fourTwelveEx = true;

        while (fourTwelveEx)
        {
        try {

            List<JobInfoEntity> entities;
            JobInfoEntity oldestEntity = null;

            entities = DynamoMapper.scan(JobInfoEntity.class, scanExpression);

            if(entities.size() == 0)
            {
                return null;
            }

            if(entities.size() == 1)
            {
                oldestEntity = entities.get(0);
                Template temp = getEntityFromBlob(oldestEntity.Guid,  TemplatesFolder, Template.class);
                oldestEntity.Status = RepositoryStatus.JOB_STATUS.Generating.getValue();
                DynamoMapper.save(oldestEntity);

                return new JobRequestData(temp,  RepositoryStatus.REQUEST_TYPE.forValue(oldestEntity.getType()), oldestEntity.CreationDate);
            }
            Collections.sort(entities, new SortByDate());
            oldestEntity = entities.get(0);

            oldestEntity.Status = RepositoryStatus.JOB_STATUS.Generating.getValue();

            fourTwelveEx = false;

            Log.info("[S3StorageManager] Updated job entity [{oldestEntity.Guid}] to generating.");

            Template temp = getEntityFromBlob(oldestEntity.Guid,  TemplatesFolder, Template.class);

            JobRequestData ret = new JobRequestData(temp,  RepositoryStatus.REQUEST_TYPE.valueOf(String.valueOf(oldestEntity.Type)), oldestEntity.CreationDate);

            return ret;

        }
        catch (Exception ex)
        {
            Log.error("[S3StorageManager] getOldestJobAndGenerate() threw an error when trying to generate oldest jobs: "+ex);
            return null;
        }
        }
        return null;
    }

    private <T> T getEntityFromBlob(String guid, String bucketName, Class<T> typeParameterClass) {
        try{
            GetObjectRequest request = new GetObjectRequest(bucketName, guid);

            S3Object res = S3Client.getObject(request);

            ObjectMapper objectMapper = new ObjectMapper();

            T obj = objectMapper.readValue(res.getObjectContent().getDelegateStream(), typeParameterClass);

            return obj;
        }
        catch(Exception ex)
        {
            Log.error("[S3StorageManager] getEntityFromBlob() threw an error when trying to get entity from S3: "+ex);
        }

        return null;
    }

    public Document getGeneratedReport(String guid)
    {
        return getEntityFromBlob(guid, DocumentsFolder, Document.class);
    }

    public ServiceError getError(String guid)
    {
        return getEntityFromBlob(guid, DocumentsFolder, ServiceError.class);
    }

    public Metrics getMetrics(String guid)
    {
        return getEntityFromBlob(guid, DocumentsFolder, Metrics.class);
    }

    public TagTree getTagTree(String guid)
    {
        return getEntityFromBlob(guid, DocumentsFolder, TagTree.class);
    }

    public JobInfoEntity getRequestInfo(String guid)
    {

        JobInfoEntity res = DynamoMapper.load(JobInfoEntity.class, guid);
        return res;
    }

    private void createClient(BasicAWSCredentials aWSCredentials) {
        Client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).withCredentials(new AWSStaticCredentialsProvider(aWSCredentials)).build();
        DynamoDb = new DynamoDB(Client);
        DynamoMapper = new DynamoDBMapper(Client);
    }
    static class SortByDate implements Comparator<JobInfoEntity> {
        @Override
        public int compare(JobInfoEntity a, JobInfoEntity b) {
            return a.CreationDate.compareTo(b.CreationDate);
        }
    }
}


