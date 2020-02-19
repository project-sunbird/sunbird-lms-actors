package org.sunbird.learner.actors.fileuploadservice;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.*;
import org.sunbird.common.models.util.azure.CloudService;
import org.sunbird.common.models.util.azure.CloudServiceFactory;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;

/** Class to upload the file on cloud storage. Created by arvind on 28/8/17. */
@ActorConfig(
  tasks = {"fileStorageService"},
  asyncTasks = {}
)
public class FileUploadServiceActor extends BaseActor {

  @Override
  public void onReceive(Request request) throws Throwable {
    if (request.getOperation().equalsIgnoreCase(ActorOperations.FILE_STORAGE_SERVICE.getValue())) {
      processFileUpload(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  private void processFileUpload(Request actorMessage) throws IOException {
    String processId = ProjectUtil.getUniqueIdFromTimestamp(1);
    Map<String, Object> req = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.DATA);

    Response response = new Response();
    String fileExtension = "";
    String fileName = (String) req.get(JsonKey.FILE_NAME);
    if (!StringUtils.isBlank(fileName)) {
      String[] split = fileName.split("\\.");
      if (split.length > 1) {
        fileExtension = split[split.length - 1];
      }
    }
    fileName = fileName.replace("."+fileExtension,"");
    String fName = fileName +"-"+ processId;
    if (!StringUtils.isBlank(fileExtension)) {
      fName = fName + "." + fileExtension.toLowerCase();
      ProjectLogger.log("File - " + fName + " Extension is " + fileExtension);
    }
    ProjectLogger.log("File name is :: "+fName, LoggerEnum.INFO.name());
    File file = new File(fName);
    FileOutputStream fos = null;
    String avatarUrl = null;
    try {
      fos = new FileOutputStream(file);
      fos.write((byte[]) req.get(JsonKey.FILE));

      CloudService service = (CloudService) CloudServiceFactory.get("Azure");
      if (null == service) {
        ProjectLogger.log("The cloud service is not available");
        ProjectCommonException exception =
            new ProjectCommonException(
                ResponseCode.invalidRequestData.getErrorCode(),
                ResponseCode.invalidRequestData.getErrorMessage(),
                ResponseCode.CLIENT_ERROR.getResponseCode());
        sender().tell(exception, self());
      }
      String container = (String) req.get(JsonKey.CONTAINER);
      avatarUrl = service.uploadFile(container, file);
    } catch (IOException e) {
      ProjectLogger.log("Exception Occurred while reading file in FileUploadServiceActor", e);
      throw e;
    } finally {
      try {
        if (ProjectUtil.isNotNull(fos)) {
          fos.close();
        }
        if (ProjectUtil.isNotNull(file)) {
          file.delete();
        }
      } catch (IOException e) {
        ProjectLogger.log(
            "Exception Occurred while closing fileInputStream in FileUploadServiceActor", e);
      }
    }
    response.put(JsonKey.URL, avatarUrl);
    sender().tell(response, self());
  }
}
