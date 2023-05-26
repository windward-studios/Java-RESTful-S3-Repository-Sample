package S3Storage;

import WindwardModels.*;
import WindwardRepository.RepositoryStatus;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Semaphore;


/**
 * NOTE: This is sample code and is not production ready. It is not optimized to run at scale. Intended for reference only
 * for your own implementation.
 */

public class S3StorageManager {

    private static AmazonDynamoDB client;
    protected static DynamoDB dynamoDB;
    private AmazonS3Client s3Client;
    private DynamoDBMapper dynamoDBMapper;

    private static String bucketName;
    private static String documentsFolder;
    private static String templatesFolder;

    private Semaphore semaphore;

    private static final Logger log = LogManager.getLogger(S3StorageManager.class);


    public S3StorageManager(BasicAWSCredentials aWSCredentials,  AmazonS3Client s3Client, String bucketName)
    {
        createClient(aWSCredentials);
        S3StorageManager.bucketName = bucketName;
        documentsFolder = "Documents/";
        templatesFolder = "Templates/";
        this.s3Client = s3Client;

        semaphore = new Semaphore(1);
    }

    public boolean AddRequest(JobRequestData requestData)
    {
        JobInfoEntity entity = JobInfoEntity.FromJobRequestData(requestData);
        entity.Status = (int)RepositoryStatus.JOB_STATUS.Pending.getValue();

        try
        {
            semaphore.acquire();
            dynamoDBMapper.save(entity);

            log.debug("[S3StorageManager AddRequest] Added template ["+ requestData.Template.getGuid() +"] to blob storage");

            ObjectMapper mapper = new ObjectMapper();
            String test = mapper.writeValueAsString(requestData.Template);

            InputStream stream = new ByteArrayInputStream(test.getBytes
                    (Charset.forName("UTF-8")));

            PutObjectRequest objectRequest = new PutObjectRequest(bucketName, templatesFolder+entity.Guid, stream, null);

            s3Client.putObject(objectRequest);
            log.debug("[S3StorageManager AddRequest] Successfully added template ["+ requestData.Template.getGuid() +"]");

            return true;
        }
        catch (Exception ex)
        {
            log.error("[S3StorageManager AddRequest] Error adding request: ", ex);
            return false;
        }
        finally {
            semaphore.release();
        }
    }


    public boolean UpdateRequest(String guid, RepositoryStatus.JOB_STATUS newStatus)
    {
        JobInfoEntity entity = getRequestInfo(guid);
        entity.Status = (int)newStatus.getValue();
        boolean success = false;
        try
        {
            dynamoDBMapper.save(entity);
            log.info("[S3StorageManager] Updated request: " + guid);
            success = true;
            return success;
        }
        catch (Exception ex)
        {
            log.error("[S3StorageManager] Error updating request: ", ex);
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

        PutObjectRequest req = new PutObjectRequest(bucketName, documentsFolder+ guid, stream, new ObjectMetadata());
        try {
            semaphore.acquire();
            s3Client.putObject(req);
            dynamoDBMapper.save(entity);

            log.info(String.format("[S3StorageManager completeRequest] Completed request [%s]", guid));

            return true;
        }
        catch (AmazonServiceException | InterruptedException ex)
        {
            log.error("[S3StorageManager completeRequest threw an error when trying to put object in S3: "+ex);
            return false;
        }
        finally
        {
            semaphore.release();
        }

    }

    public boolean deleteRequest(JobInfoEntity job)
    {
        try {

            dynamoDBMapper.delete(job);

            s3Client.deleteObject(documentsFolder, job.Guid);
            s3Client.deleteObject(templatesFolder, job.Guid);

            return true;
        }
        catch (Exception ex)
        {
            log.error("[S3StorageManager] deleteRequest threw an error when trying to delete object in dynamo table: "+ex);
            return false;
        }
    }

    public boolean revertGeneratingJobsPending()
    {
        try {

            semaphore.acquire();

            DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();

            scanExpression.addFilterCondition("Status", new Condition().withComparisonOperator(ComparisonOperator.EQ).
                    withAttributeValueList(new AttributeValue().withN(String.valueOf(RepositoryStatus.JOB_STATUS.Generating.getValue()))));


            List<JobInfoEntity> tmp = dynamoDBMapper.scan(JobInfoEntity.class, scanExpression);
            for ( JobInfoEntity job : tmp)
            {
                UpdateRequest(job.Guid, RepositoryStatus.JOB_STATUS.Pending);
            }
            return true;
        }
        catch (Exception ex)
        {
            log.error("[S3StorageManager] revertGeneratingJobsPending() threw an error when trying to revert generating jobs: "+ex);
            return false;
        }
        finally {
            semaphore.release();
        }
    }

    public void deleteOldRequests(LocalDateTime cutoff)
    {
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        scanExpression.addFilterCondition("CreationDate", new Condition().withComparisonOperator(ComparisonOperator.LE).
                withAttributeValueList(new AttributeValue().withN(String.valueOf(cutoff))));

        try{
            List<JobInfoEntity> tmp = dynamoDBMapper.scan(JobInfoEntity.class, scanExpression);
            for ( JobInfoEntity job : tmp)
            {
                deleteRequest(job);
            }
        }
        catch (Exception ex)
        {
            log.error("[S3StorageManager] deleteOldRequests() threw an error when trying to delete old jobs: "+ex);
        }
    }

    public JobRequestData getOldestJobAndGenerate()
    {
        try {

            semaphore.acquire();

            DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();

            scanExpression.addFilterCondition("Status", new Condition().withComparisonOperator(ComparisonOperator.EQ).
                withAttributeValueList(new AttributeValue().withN(String.valueOf(RepositoryStatus.JOB_STATUS.Pending.getValue()))));

            List<JobInfoEntity> entities = new ArrayList<>(dynamoDBMapper.scan(JobInfoEntity.class, scanExpression, new DynamoDBMapperConfig(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)));

            log.info(String.format("[S3StorageManager] Found %d pending jobs to be processed", entities.size()));

            if(entities.size() == 0)
                return null;

            return retrieveOldestRequest(entities);
        }
        catch (Exception ex)
        {
            log.error("[S3StorageManager] getOldestJobAndGenerate() threw an error when trying to generate oldest jobs: "+ex);
            return null;
        }
        finally {
            semaphore.release();
        }
    }

    /**
     * Sorts entities and returns the oldest request to be processed
     * @param entities The current outstanding requests to be processed
     * @return The entity to be processed
     */
    private JobRequestData retrieveOldestRequest(List<JobInfoEntity> entities) {
        Collections.sort(entities, new SortByDate());

        for(int i = 0; i < entities.size(); i++) {
            JobInfoEntity oldestEntity = entities.get(i);
            Template template = getEntityFromBlob(oldestEntity.Guid, templatesFolder, Template.class);
            if(template == null) {
                deleteRequest(oldestEntity);
                log.info(String.format("[S3StorageManager] Deleted job entity [%s] due to null template.", oldestEntity.getGuid()));
                continue;   // If the template is null, delete the request and continue to next oldest request
            }

            log.info(String.format("[S3StorageManager] Updated job entity [%s] to generating.", oldestEntity.getGuid()));

            oldestEntity.Status = RepositoryStatus.JOB_STATUS.Generating.getValue();
            dynamoDBMapper.save(oldestEntity);

            return new JobRequestData(template,  RepositoryStatus.REQUEST_TYPE.forValue(oldestEntity.Type), oldestEntity.CreationDate);
        }

        // No entities to be processed
        log.info("[S3StorageManager] No entities to be processed.");
        return null;
    }

    private <T> T getEntityFromBlob(String guid, String folderName, Class<T> typeParameterClass) {
        try{
            GetObjectRequest request = new GetObjectRequest(bucketName, folderName+guid);

            S3Object res = s3Client.getObject(request);

            ObjectMapper objectMapper = new ObjectMapper();

            T obj = objectMapper.readValue(res.getObjectContent().getDelegateStream(), typeParameterClass);

            return obj;
        }
        catch(Exception ex)
        {
            log.error("[S3StorageManager] getEntityFromBlob() threw an error when trying to get entity from S3: "+ex);
        }

        return null;
    }

    public Document getGeneratedReport(String guid)
    {
        return getEntityFromBlob(guid, documentsFolder, Document.class);
    }

    public ServiceError getError(String guid)
    {
        return getEntityFromBlob(guid, documentsFolder, ServiceError.class);
    }

    public Metrics getMetrics(String guid)
    {
        return getEntityFromBlob(guid, documentsFolder, Metrics.class);
    }

    public TagTree getTagTree(String guid)
    {
        return getEntityFromBlob(guid, documentsFolder, TagTree.class);
    }

    public JobInfoEntity getRequestInfo(String guid)
    {
        String rangeKey = guid + JobInfoEntity.RANGE_KEY_EXT;
        JobInfoEntity res = dynamoDBMapper.load(JobInfoEntity.class, guid, rangeKey, new DynamoDBMapperConfig(DynamoDBMapperConfig.ConsistentReads.CONSISTENT));
        return res;
    }

    private void createClient(BasicAWSCredentials aWSCredentials) {
        ClientConfiguration cf = new ClientConfiguration().withConnectionTimeout(2000).withClientExecutionTimeout(2000).withRequestTimeout(2000).withSocketTimeout(2000).withRetryPolicy(PredefinedRetryPolicies.getDynamoDBDefaultRetryPolicyWithCustomMaxRetries(15));
        client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).withCredentials(new AWSStaticCredentialsProvider(aWSCredentials)).withClientConfiguration(cf).build();
        dynamoDB = new DynamoDB(client);
        dynamoDBMapper = new DynamoDBMapper(client);
    }
    static class SortByDate implements Comparator<JobInfoEntity> {
        @Override
        public int compare(JobInfoEntity a, JobInfoEntity b) {
            return a.CreationDate.compareTo(b.CreationDate);
        }
    }
}


