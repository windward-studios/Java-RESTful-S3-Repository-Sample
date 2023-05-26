package S3RepositoryPlugin;

import S3Storage.JobInfoEntity;
import S3Storage.JobRequestData;
import S3Storage.S3StorageManager;
import WindwardModels.*;
import WindwardRepository.*;
import bolts.CancellationToken;
import bolts.CancellationTokenSource;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import net.windward.util.LicenseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * NOTE: This is sample code and is not production ready. It is not optimized to run at scale. Intended for reference only
 * for your own implementation.
 */

public class S3Repository implements IRepository {

    private String bucketName = "";
    private String awsAccessKey = "";
    private String awsSecretKey = "";

    BasicAWSCredentials aWSCredentials;
    S3StorageManager storageManager;
    private AmazonS3Client s3Client;


    private AutoResetEvent eventSignal;
    private boolean shutDown;
    private Duration timeSpanDeleteOldJobs = Duration.ZERO;
    private Duration timeSpanCheckOldJobs = Duration.ZERO;
    private LocalDateTime datetimeLastCheckOldJobs = LocalDateTime.MIN;

    private static final Logger Log = LogManager.getLogger(S3Repository.class);

    public S3Repository()
    {
        Log.info("Initializing S3Repository");
        aWSCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        s3Client = (AmazonS3Client) AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(aWSCredentials)).withRegion(Regions.US_EAST_1).build();
        storageManager = new S3StorageManager(aWSCredentials, s3Client, bucketName);
        int hours;
        try {
            hours = Integer.parseInt(System.getProperty("hours-delete-jobs"));
        } catch (NumberFormatException e) {
            hours = 24;
        }
        timeSpanDeleteOldJobs = Duration.ofHours(hours);

        timeSpanCheckOldJobs = Duration.ofMinutes(timeSpanDeleteOldJobs.toMinutes() / 96);

        datetimeLastCheckOldJobs = LocalDateTime.now();

        if (Log.isInfoEnabled()) {
            Log.info(
                    String.format("starting FileSystemRepository, Delete jobs older than %1$s", timeSpanDeleteOldJobs));
        }


        eventSignal = new AutoResetEvent(true);

        // This thread manages all the background threads. It sleeps on an event and
        // when awoken, fires off anything it can.
        // This is used so web requests that call signal aren't delayed as background
        // tasks might be started.
        CancellationTokenSource tokenSource = new CancellationTokenSource();
        CancellationToken token = tokenSource.getToken();

        token.register(() -> eventSignal.set());

        new Thread((new Runnable() {
            CancellationToken cancelToken;

            public Runnable pass(CancellationToken ct) {
                this.cancelToken = ct;
                return this;
            }

            public void run() {
                manageRequests(cancelToken);
            }
        }.pass(token))).start();

    }

    private IJobHandler JobHandler;

    private IJobHandler getJobHandler() {
        return JobHandler;
    }

    @Override
    public final void SetJobHandler(IJobHandler handler) {
        if (Log.isDebugEnabled())
            Log.debug(String.format("SetJobHandler(%1$s)", handler));
        JobHandler = handler;
    }

    @Override
    public void shutDown() {
        boolean tmp = storageManager.revertGeneratingJobsPending();
    }

    private void manageRequests(CancellationToken cancelToken) {
        while (!shutDown && !cancelToken.isCancellationRequested()) {
            if (datetimeLastCheckOldJobs.plus(timeSpanCheckOldJobs).isBefore(LocalDateTime.now())) {
                deleteOldJobs();
                datetimeLastCheckOldJobs = LocalDateTime.now();
            }

            // wait until needed again, or cancelled, or time to check for jobs.
            try {
                eventSignal.waitOne(timeSpanCheckOldJobs.toMillis());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String createRequest(Template template, RepositoryStatus.REQUEST_TYPE request_type) {
        try {
            template.setGuid(UUID.randomUUID().toString());
            JobRequestData jobData = new JobRequestData(template, request_type, LocalDateTime.now());
            Log.info("[S3RepoPlugin] Created request " + jobData.Template.getGuid());

            storageManager.AddRequest(jobData);

            if (getJobHandler() != null) {
                getJobHandler().Signal();
            }

            return template.getGuid();
        }
        catch(Exception ex)
        {
            Log.error("[S3RepoPlugin] createRequest() Failed to created request " + template.getGuid());
        }

        return null;
    }

    @Override
    public RepositoryRequest takeRequest() {

        Log.info("[S3RepoPlugin] Take request called");
        try
        {
            JobRequestData job = storageManager.getOldestJobAndGenerate();
            if(job != null)
            {
                Log.info("[S3RepoPlugin] Took requests: "+job.Template.getGuid());
                return new RepositoryRequest(job.Template, job.RequestType);
            }
            else
            {
                Log.info("[S3RepoPlugin] takeRequest() returning null. No requests are in the queue.");
                return null;
            }
        }
        catch (Exception ex)
        {
            Log.error("[S3RepoPlugin] TakeRequest Error in taking request: " +ex);
        }
        return null;
    }

    @Override
    public void saveReport(Template template, Document document) throws IOException
    {
        boolean res = storageManager.completeRequest(template.getGuid(), document);

        if(res)
        {
            Log.info("[S3RepoPlugin] saveRequest() saved request successfully: "+template.getGuid());
            completeJob(template);
        }
        else
        {
            Log.error("[S3RepoPlugin] saveRequest() error saving request: "+template.getGuid());
        }
    }


    private void completeJob(Template template)
    {
        if (shutDown || (template.getCallback() == null  || template.getCallback().length() == 0))
            return;
        String url = template.getCallback().replace("{guid}", template.getGuid());
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;
        try {
            HttpPost post = new HttpPost(url);

            httpClient = HttpClients.createDefault();
            response = httpClient.execute(post);
            if (response.getStatusLine().getStatusCode() != 200 && Log.isInfoEnabled()) {
                Log.info(String.format("Callback to %1$s returned status code %2$s", url,
                        response.getStatusLine().getStatusCode()));
            }
            httpClient.close();
            response.close();
        } catch (RuntimeException | IOException ex) {
            try {
                httpClient.close();
                response.close();
            } catch (Exception e) {
                // nothing to do here
            }
            Log.warn(String.format("Callback for job %1$s to url %2$s threw exception %3$s", template.getGuid(),
                    template.getCallback(), ex.getMessage()), ex);
            // silently swallow the exception - this is a background thread.
        }

    }

    @Override
    public void saveError(Template template, ServiceError serviceError)
    {
        try
        {
            boolean result;
            if(serviceError.getType().equals("net.windward.util.LicenseException")){
                result = storageManager.UpdateRequest(template.getGuid(), RepositoryStatus.JOB_STATUS.LicenseError);
            }
            else {
                result = storageManager.UpdateRequest(template.getGuid(), RepositoryStatus.JOB_STATUS.Error);
            }

            if(!result) {
                Log.error("[S3RepoPlugin] saveError() error saving error status: "+template.getGuid());
            }

            result = storageManager.completeRequest(template.getGuid(), serviceError);

            if(result) {
                Log.info("[S3RepoPlugin] saveError() Successfully saved error status: "+template.getGuid());
            }
            else {
                Log.error("[S3RepoPlugin] saveError() error saving error status: "+template.getGuid());
            }
        }
        catch (IOException ex)
        {
            Log.error("[S3RepoPlugin] saveError() error saving error status: "+template.getGuid(), ex);
        }
    }

    @Override
    public void saveTagTree(Template template, TagTree tagTree)
    {
        try
        {
            boolean res = storageManager.completeRequest(template.getGuid(), tagTree);
            if(res)
            {
                Log.info("[S3RepoPlugin] saveTagTree() saved tagTree successfully: "+template.getGuid());
                completeJob(template);
            }
            else
            {
                Log.error("[S3RepoPlugin] saveTagTree() error saving tagtree: "+template.getGuid());
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveMetrics(Template template, Metrics metrics)
    {
        boolean res;
        try
        {
            res = storageManager.completeRequest(template.getGuid(), metrics);
            if(res)
            {
                Log.info("[S3RepoPlugin] saveMetrics() saved metrics successfully: "+template.getGuid());
                completeJob(template);
            }
            else
            {
                Log.error("[S3RepoPlugin] saveMetrics() error saving metrics: "+template.getGuid());
            }
        }
        catch (IOException e) {
            Log.error("[S3RepoPlugin] saveMetrics() error saving metrics: "+template.getGuid(), e);
        }

    }

    @Override
    public RequestStatus getReportStatus(String guid) {
        try
        {
            JobInfoEntity result = storageManager.getRequestInfo(guid);
            return new RequestStatus(RepositoryStatus.JOB_STATUS.forValue(result.Status), RepositoryStatus.REQUEST_TYPE.forValue(result.Type));
        }
        catch(Exception ex) {
            Log.error("[S3RepoPlugin] saveMetrics() error getting report status: "+ guid, ex);
        }
        return null;
    }

    @Override
    public Document getReport(String guid) {
        try
        {
            Document result = storageManager.getGeneratedReport(guid);
            Log.info("[S3RepoPlugin] getReport() got report Successfully: "+ guid);
            return result;

        }
        catch (Exception ex)
        {
            Log.error("[S3RepoPlugin] getReport() error getting report: "+guid, ex);
        }
        return null;
    }

    @Override
    public ServiceError getError(String guid) {
        try
        {
            ServiceError result = storageManager.getError(guid);
            Log.info("[S3RepoPlugin] getError() got error Successfully: "+ guid);
            return result;

        }
        catch (Exception ex)
        {
            Log.error("[S3RepoPlugin] getError() error getting error: "+guid, ex);
        }
        return null;
    }

    @Override
    public TagTree getTagTree(String guid) {
        try
        {
            TagTree result = storageManager.getTagTree(guid);
            Log.info("[S3RepoPlugin] getTagTree() got TagTree Successfully: "+ guid);
            return  result;

        }
        catch (Exception ex)
        {
            Log.error("[S3RepoPlugin] getTagTree() error getting TagTree: "+guid, ex);
        }
        return null;
    }

    @Override
    public Metrics getMetrics(String guid) {
        try
        {
            Metrics result = storageManager.getMetrics(guid);
            Log.info("[S3RepoPlugin] getMetrics() got metrics Successfully: "+ guid);
            return result;

        }
        catch (Exception ex)
        {
            Log.error("[S3RepoPlugin] getMetrics() error getting metrics: "+guid, ex);
        }
        return null;
    }

    @Override
    public void deleteReport(String guid)
    {
        try
        {
            JobInfoEntity jobInfo = storageManager.getRequestInfo(guid);
            storageManager.deleteRequest(jobInfo);
            Log.error("[S3RepoPlugin] deleteReport() successfully deleted report: "+guid);
        }
        catch (Exception ex)
        {
            Log.error("[S3RepoPlugin] deleteReport() error deleting report: "+guid, ex);
        }
    }

    private void deleteOldJobs() {
        LocalDateTime oldDateTime = LocalDateTime.now().minus(timeSpanDeleteOldJobs);
        storageManager.deleteOldRequests(oldDateTime);
    }

    @Override
    public DocumentMeta getMeta(String guid)
    {
        try {
            Document doc = storageManager.getGeneratedReport(guid);
            DocumentMeta docMeta = setReportMeta(doc);
            return docMeta;
        }
        catch (Exception ex)
        {
            Log.error("[S3RepoPlugin] getMeta() error getting document meta: "+guid, ex);
        }
        return null;
    }

    private DocumentMeta setReportMeta(Document doc) {
        DocumentMeta docMeta = new DocumentMeta();
        docMeta.setGuid(doc.getGuid());
        docMeta.setNumberOfPages(doc.getNumberOfPages());
        docMeta.setImportInfo(doc.getImportInfo());
        docMeta.setTag(doc.getTag());
        docMeta.setErrors(doc.getErrors());

        return docMeta;
    }


    /*
    * Implement the following 2 methods to make use of document performance feature. https://fluent.apryse.com/documentation/engine-guide/Fluent%20RESTful%20Engines/JavaRestSotragePlugin#methods-and-variables
    */

    @Override
    public void saveDocumentPerformanceObject(DocumentPerformance documentPerformance, String s) {

    }

    @Override
    public DocumentPerformance getDocumentPerformanceObject(String s) {
        return null;
    }


    /*
    * Implement the following methods to make use of cached resource feature. https://fluent.apryse.com/documentation/engine-guide/Fluent%20RESTful%20Engines/JavaRestSotragePlugin#methods-and-variables
     */
    @Override
    public void saveCachedResource(CachedResource cachedResource) throws Exception {

    }

    @Override
    public net.windward.util.AccessProviders.models.CachedResource getCachedResource(String s) {
        return null;
    }

    @Override
    public void deleteCachedResource(String s) throws FileNotFoundException {

    }
}
