package S3Storage;


import WindwardModels.Template;
import WindwardRepository.RepositoryStatus;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.time.LocalDateTime;


/**
 * NOTE: This is sample code and is not production ready. It is not optimized to run at scale. Intended for reference only
 * for your own implementation.
 */

@XmlRootElement(name="JobRequestData")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobRequestData implements Serializable {

    @XmlElement(name = "Template")
    public Template Template;

    @XmlElement(name = "RequestType")
    public RepositoryStatus.REQUEST_TYPE RequestType;

    @XmlElement(name = "CreationDate")
    public LocalDateTime CreationDate;

    public JobRequestData(Template template, RepositoryStatus.REQUEST_TYPE requestType, LocalDateTime creationDate)
    {
        this.Template = template;
        this.RequestType = requestType;
        this.CreationDate = creationDate;
    }

    public void setTemplate(WindwardModels.Template template) {
        Template = template;
    }
    public Template getTemplate() { return  this.Template; }

    public void setRequestType(RepositoryStatus.REQUEST_TYPE requestType) {
        RequestType = requestType;
    }

    public RepositoryStatus.REQUEST_TYPE getRequestType() {
        return RequestType;
    }

    public void setCreationDate(LocalDateTime creationDate) {
        CreationDate = creationDate;
    }

    public LocalDateTime getCreationDate() {
        return CreationDate;
    }
}
