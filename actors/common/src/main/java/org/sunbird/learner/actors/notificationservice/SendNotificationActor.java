package org.sunbird.learner.actors.notificationservice;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.sunbird.actor.background.BackgroundOperations;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.actors.notificationservice.dao.EmailTemplateDao;
import org.sunbird.learner.actors.notificationservice.dao.impl.EmailTemplateDaoImpl;
import org.sunbird.learner.util.Util;

import java.text.MessageFormat;
import java.util.*;

@ActorConfig(
  tasks = {"v2Notification"},
  asyncTasks = {}
)
public class SendNotificationActor extends BaseActor {
  @Override
  public void onReceive(Request request) throws Throwable {
    if (request.getOperation().equalsIgnoreCase(ActorOperations.V2_NOTIFICATION.getValue())) {
      sendNotification(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  private void sendNotification(Request request) {
    Map<String, Object> requestMap =
      (Map<String, Object>) request.getRequest().get(JsonKey.EMAIL_REQUEST);
    List<String> userIds = (List<String>) requestMap.remove(JsonKey.RECIPIENT_USERIDS);
    List<String> phoneOrEmailList;
    Map<String,Object> notificationReq;
    String mode = (String) requestMap.remove(JsonKey.MODE);
    ProjectLogger.log("SendNotificationActor:sendNotification : called for mode =" + mode, LoggerEnum.INFO.name());
    if (StringUtils.isNotBlank(mode)
      && JsonKey.SMS.equalsIgnoreCase(mode)) {
      phoneOrEmailList = getUsersEmailOrPhone(userIds, JsonKey.PHONE);
      notificationReq = getNotificationRequest(phoneOrEmailList,requestMap,JsonKey.SMS,null);
    } else {
      phoneOrEmailList = getUsersEmailOrPhone(userIds, JsonKey.EMAIL);
      String template = getEmailTemplateFile((String) requestMap.get(JsonKey.EMAIL_TEMPLATE_TYPE));
      notificationReq = getNotificationRequest(phoneOrEmailList,requestMap,JsonKey.EMAIL,template);
    }
    ProjectLogger.log("SendNotificationActor:sendNotification : called for userIds =" + userIds, LoggerEnum.INFO.name());
    process(notificationReq,request.getRequestId());

    Response res = new Response();
    res.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    sender().tell(res, self());
  }

  private void process(Map<String, Object> notificationReq, String requestId) {
    List<Map<String,Object>> notificationList = new ArrayList<>();
    notificationList.add(notificationReq);
    Map<String,Object> reqMap = new HashMap<>();
    Map<String,Object> request = new HashMap<>();
    request.put("notifications",notificationList);
    reqMap.put(JsonKey.REQUEST,request);

    Request bgRequest = new Request();
    bgRequest.setRequestId(requestId);
    bgRequest.getRequest().putAll(reqMap);
    bgRequest.setOperation("processNotification");
    tellToAnother(bgRequest);
  }

  private Map<String, Object> getNotificationRequest(List<String> phoneOrEmailList, Map<String, Object> requestMap, String mode, String template) {
    Map<String, Object> notiReq = new HashMap<>();
    notiReq.put("deliveryType","message");
    Map<String, Object> config = new HashMap<>(2);
    config.put("sender",System.getenv("sunbird_mail_server_from_email"));
    config.put(JsonKey.SUBJECT,requestMap.remove(JsonKey.SUBJECT));
    notiReq.put("config",config);
    Map<String, Object> templateMap = new HashMap<>(2);
    if (mode.equalsIgnoreCase(JsonKey.SMS)) {
      templateMap.put(JsonKey.DATA,requestMap.remove(JsonKey.BODY));
      templateMap.put(JsonKey.PARAMS,Collections.EMPTY_MAP);
      notiReq.put("template",templateMap);
      notiReq.put(JsonKey.MODE,JsonKey.PHONE);
    } else {
      templateMap.put(JsonKey.DATA,template);
      templateMap.put(JsonKey.PARAMS,requestMap);
      notiReq.put("template",templateMap);
      notiReq.put(JsonKey.MODE,JsonKey.EMAIL);
    }
    notiReq.put("ids",phoneOrEmailList);
    return notiReq;
  }

  private String getEmailTemplateFile(String templateName) {
    EmailTemplateDao emailTemplateDao = EmailTemplateDaoImpl.getInstance();
    String template = emailTemplateDao.getTemplate(templateName);
    if (StringUtils.isBlank(template)) {
      ProjectCommonException.throwClientErrorException(
        ResponseCode.invalidParameterValue,
        MessageFormat.format(
          ResponseCode.invalidParameterValue.getErrorMessage(),
          templateName,
          JsonKey.EMAIL_TEMPLATE_TYPE));
    }
    return template;
  }

  private List<String> getUsersEmailOrPhone(List<String> userIds, String key) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    List<String> fields = new ArrayList<>();
    fields.add(key);
    Response response =
      ServiceFactory.getInstance().getRecordsByIdsWithSpecifiedColumns(
        usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), fields, userIds);
    List<Map<String, Object>> userList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    return getPhoneOrEmailList(userList, key);
  }

  private List<String> getPhoneOrEmailList(List<Map<String, Object>> userList, String key) {
    if (CollectionUtils.isNotEmpty(userList)) {
      DecryptionService decryptionService =
        org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(
          null);
      List<String> emailOrPhoneList = new ArrayList<>();
      for (Map<String, Object> userMap : userList) {
        String email = (String) userMap.get(key);
        if (StringUtils.isNotBlank(email)) {
          String decryptedEmail = decryptionService.decryptData(email);
          emailOrPhoneList.add(decryptedEmail);
        }
      }
      if (CollectionUtils.isNotEmpty(emailOrPhoneList)) {
        return emailOrPhoneList;
      } else {
        ProjectCommonException.throwClientErrorException(
          ResponseCode.notificationNotSent,
          MessageFormat.format(
            ResponseCode.notificationNotSent.getErrorMessage(),
            key));
      }
    }
    return Collections.EMPTY_LIST;
  }

}
