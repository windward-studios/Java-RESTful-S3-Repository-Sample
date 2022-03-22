package S3Storage;

import com.amazonaws.services.dynamodbv2.datamodeling.*;

import java.io.Serializable;
import java.time.LocalDateTime;

@DynamoDBTable(tableName="YOUR_TABLE_NAME")
public class JobInfoEntity implements Serializable {

    public String Guid;
    public int Type;
    public int Status;
    public LocalDateTime CreationDate;

    public  JobInfoEntity()
    {

    }
    public JobInfoEntity(String guid, int type, int status, LocalDateTime creationDate)
    {
        this.Guid = guid;
        this.Type = type;
        this.Status = status;
        this.CreationDate = creationDate;
    }
    public static JobInfoEntity FromJobRequestData(JobRequestData data)
    {
        return new JobInfoEntity(data.getTemplate().getGuid(), (int)data.RequestType.getValue(), -1, data.getCreationDate());
    }

    @DynamoDBHashKey(attributeName="Guid")
    public String getGuid() {
        return Guid;
    }
    public void setGuid(String guid) {
        Guid = guid;
    }

    @DynamoDBAttribute(attributeName="Type")
    public int getType() {
        return Type;
    }
    public void setType(int type) {
        Type = type;
    }

    @DynamoDBAttribute(attributeName="Status")
    public int getStatus() {
        return Status;
    }
    public void setStatus(int status) {
        Status = status;
    }

    @DynamoDBAttribute(attributeName="CreationDate")
    @DynamoDBTypeConverted( converter = LocalDateTimeConverter.class )
    public LocalDateTime getCreationDate() {
        return CreationDate;
    }
    public void setCreationDate(LocalDateTime creationDate) {
        CreationDate = creationDate;
    }

    static public class LocalDateTimeConverter implements DynamoDBTypeConverter<String, LocalDateTime> {

        @Override
        public String convert( final LocalDateTime time ) {

            return time.toString();
        }

        @Override
        public LocalDateTime unconvert( final String stringValue ) {

            return LocalDateTime.parse(stringValue);
        }
    }
}
