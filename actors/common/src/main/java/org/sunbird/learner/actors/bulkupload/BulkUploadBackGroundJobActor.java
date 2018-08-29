package org.sunbird.learner.actors.bulkupload;

import static org.sunbird.learner.util.Util.isNotNull;
import static org.sunbird.learner.util.Util.isNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LocationActorOperation;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.BulkProcessStatus;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.ProjectUtil.Status;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.models.util.Slug;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.UserRequestValidator;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.AuditOperation;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.UserUtility;
import org.sunbird.learner.util.Util;
import org.sunbird.models.organization.Organization;
import org.sunbird.models.user.User;
import org.sunbird.services.sso.SSOManager;
import org.sunbird.services.sso.SSOServiceFactory;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.validator.location.LocationRequestValidator;

/**
 * This actor will handle bulk upload operation .
 *
 * @author Amit Kumar
 */
@ActorConfig(
  tasks = {},
  asyncTasks = {"processBulkUpload"}
)
public class BulkUploadBackGroundJobActor extends BaseActor {

  private String processId = "";
  private final Util.DbInfo bulkDb = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
  private final EncryptionService encryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getEncryptionServiceInstance(
          null);
  private final DecryptionService decryptionService =
      org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.getDecryptionServiceInstance(
          null);
  private final PropertiesCache propertiesCache = PropertiesCache.getInstance();
  private final List<String> locnIdList = new ArrayList<>();
  private final CassandraOperation cassandraOperation = ServiceFactory.getInstance();
  private final SSOManager ssoManager = SSOServiceFactory.getInstance();
  private ObjectMapper mapper = new ObjectMapper();
  private static final LocationRequestValidator validator = new LocationRequestValidator();

  @Override
  public void onReceive(Request request) throws Throwable {
    Util.initializeContext(request, JsonKey.USER);
    ExecutionContext.setRequestId(request.getRequestId());
    if (request.getOperation().equalsIgnoreCase(ActorOperations.PROCESS_BULK_UPLOAD.getValue())) {
      process(request);
    } else {
      onReceiveUnsupportedOperation(request.getOperation());
    }
  }

  private void process(Request actorMessage) {
    processId = (String) actorMessage.get(JsonKey.PROCESS_ID);
    Map<String, Object> dataMap = getBulkData(processId);
    int status = (int) dataMap.get(JsonKey.STATUS);
    if (!(status == (ProjectUtil.BulkProcessStatus.COMPLETED.getValue())
        || status == (ProjectUtil.BulkProcessStatus.INTERRUPT.getValue()))) {
      TypeReference<List<Map<String, Object>>> mapType =
          new TypeReference<List<Map<String, Object>>>() {};
      List<Map<String, Object>> jsonList = null;
      try {
        jsonList = mapper.readValue((String) dataMap.get(JsonKey.DATA), mapType);
      } catch (IOException e) {
        ProjectLogger.log(
            "Exception occurred while converting json String to List in BulkUploadBackGroundJobActor : ",
            e);
      }
      if (((String) dataMap.get(JsonKey.OBJECT_TYPE)).equalsIgnoreCase(JsonKey.USER)) {
        long startTime = System.currentTimeMillis();
        ProjectLogger.log(
            "BulkUploadBackGroundJobActor:processUserInfo start at : " + startTime,
            LoggerEnum.INFO.name());
        processUserInfo(jsonList, processId, (String) dataMap.get(JsonKey.UPLOADED_BY));
        ProjectLogger.log(
            "BulkUploadBackGroundJobActor:processUserInfo Total time taken : for processId  : "
                + processId
                + " : "
                + (System.currentTimeMillis() - startTime),
            LoggerEnum.INFO.name());
      } else if (((String) dataMap.get(JsonKey.OBJECT_TYPE))
          .equalsIgnoreCase(JsonKey.ORGANISATION)) {
        CopyOnWriteArrayList<Map<String, Object>> orgList = new CopyOnWriteArrayList<>(jsonList);
        processOrgInfo(orgList, dataMap);
      } else if (((String) dataMap.get(JsonKey.OBJECT_TYPE)).equalsIgnoreCase(JsonKey.BATCH)) {
        processBatchEnrollment(jsonList, processId);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void processBatchEnrollment(List<Map<String, Object>> jsonList, String processId) {
    // update status from NEW to INProgress
    updateStatusForProcessing(processId);
    Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
    List<Map<String, Object>> successResultList = new ArrayList<>();
    List<Map<String, Object>> failureResultList = new ArrayList<>();

    Map<String, Object> successListMap = null;
    Map<String, Object> failureListMap = null;
    for (Map<String, Object> batchMap : jsonList) {
      successListMap = new HashMap<>();
      failureListMap = new HashMap<>();
      Map<String, Object> tempFailList = new HashMap<>();
      Map<String, Object> tempSuccessList = new HashMap<>();

      String batchId = (String) batchMap.get(JsonKey.BATCH_ID);
      Response courseBatchResult =
          cassandraOperation.getRecordById(dbInfo.getKeySpace(), dbInfo.getTableName(), batchId);
      String msg = validateBatchInfo(courseBatchResult);
      if (msg.equals(JsonKey.SUCCESS)) {
        List<Map<String, Object>> courseList =
            (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
        List<String> userList =
            new ArrayList<>(Arrays.asList((((String) batchMap.get(JsonKey.USER_IDs)).split(","))));
        validateBatchUserListAndAdd(
            courseList.get(0), batchId, userList, tempFailList, tempSuccessList);
        failureListMap.put(batchId, tempFailList.get(JsonKey.FAILURE_RESULT));
        successListMap.put(batchId, tempSuccessList.get(JsonKey.SUCCESS_RESULT));
      } else {
        batchMap.put(JsonKey.ERROR_MSG, msg);
        failureResultList.add(batchMap);
      }
      if (!successListMap.isEmpty()) {
        successResultList.add(successListMap);
      }
      if (!failureListMap.isEmpty()) {
        failureResultList.add(failureListMap);
      }
    }

    // Insert record to BulkDb table
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.ID, processId);
    map.put(JsonKey.SUCCESS_RESULT, ProjectUtil.convertMapToJsonString(successResultList));
    map.put(JsonKey.FAILURE_RESULT, ProjectUtil.convertMapToJsonString(failureResultList));
    map.put(JsonKey.PROCESS_END_TIME, ProjectUtil.getFormattedDate());
    map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    try {
      cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
    } catch (Exception e) {
      ProjectLogger.log(
          "Exception Occurred while updating bulk_upload_process in BulkUploadBackGroundJobActor : ",
          e);
    }
  }

  @SuppressWarnings("unchecked")
  private void validateBatchUserListAndAdd(
      Map<String, Object> courseBatchObject,
      String batchId,
      List<String> userIds,
      Map<String, Object> failList,
      Map<String, Object> successList) {
    Util.DbInfo dbInfo = Util.dbInfoMap.get(JsonKey.COURSE_BATCH_DB);
    Util.DbInfo userOrgdbInfo = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
    List<Map<String, Object>> failedUserList = new ArrayList<>();
    List<Map<String, Object>> passedUserList = new ArrayList<>();

    Map<String, Object> map = null;
    List<String> createdFor = (List<String>) courseBatchObject.get(JsonKey.COURSE_CREATED_FOR);
    Map<String, Boolean> participants =
        (Map<String, Boolean>) courseBatchObject.get(JsonKey.PARTICIPANT);
    if (participants == null) {
      participants = new HashMap<>();
    }
    // check whether can update user or not
    for (String userId : userIds) {
      if (!(participants.containsKey(userId))) {
        Response dbResponse =
            cassandraOperation.getRecordsByProperty(
                userOrgdbInfo.getKeySpace(), userOrgdbInfo.getTableName(), JsonKey.USER_ID, userId);
        List<Map<String, Object>> userOrgResult =
            (List<Map<String, Object>>) dbResponse.get(JsonKey.RESPONSE);

        if (userOrgResult.isEmpty()) {
          map = new HashMap<>();
          map.put(userId, ResponseCode.userNotAssociatedToOrg.getErrorMessage());
          failedUserList.add(map);
          continue;
        }
        boolean flag = false;
        for (int i = 0; i < userOrgResult.size() && !flag; i++) {
          Map<String, Object> usrOrgDetail = userOrgResult.get(i);
          if (createdFor.contains(usrOrgDetail.get(JsonKey.ORGANISATION_ID))) {
            participants.put(
                userId,
                addUserCourses(
                    batchId,
                    (String) courseBatchObject.get(JsonKey.COURSE_ID),
                    userId,
                    (Map<String, String>) (courseBatchObject.get(JsonKey.COURSE_ADDITIONAL_INFO))));
            flag = true;
          }
        }
        if (flag) {
          map = new HashMap<>();
          map.put(userId, JsonKey.SUCCESS);
          passedUserList.add(map);
        } else {
          map = new HashMap<>();
          map.put(userId, ResponseCode.userNotAssociatedToOrg.getErrorMessage());
          failedUserList.add(map);
        }

      } else {
        map = new HashMap<>();
        map.put(userId, JsonKey.SUCCESS);
        passedUserList.add(map);
      }
    }
    courseBatchObject.put(JsonKey.PARTICIPANT, participants);
    cassandraOperation.updateRecord(dbInfo.getKeySpace(), dbInfo.getTableName(), courseBatchObject);
    successList.put(JsonKey.SUCCESS_RESULT, passedUserList);
    failList.put(JsonKey.FAILURE_RESULT, failedUserList);
    // process Audit Log
    processAuditLog(courseBatchObject, ActorOperations.UPDATE_BATCH.getValue(), "", JsonKey.BATCH);
    ProjectLogger.log("method call going to satrt for ES--.....");
    Request request = new Request();
    request.setOperation(ActorOperations.UPDATE_COURSE_BATCH_ES.getValue());
    request.getRequest().put(JsonKey.BATCH, courseBatchObject);
    ProjectLogger.log("making a call to save Course Batch data to ES");
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log(
          "Exception Occurred during saving Course Batch to Es while updating Course Batch : ", ex);
    }
  }

  private Boolean addUserCourses(
      String batchId, String courseId, String userId, Map<String, String> additionalCourseInfo) {

    Util.DbInfo courseEnrollmentdbInfo = Util.dbInfoMap.get(JsonKey.LEARNER_COURSE_DB);
    Util.DbInfo coursePublishdbInfo = Util.dbInfoMap.get(JsonKey.COURSE_PUBLISHED_STATUS);
    Response response =
        cassandraOperation.getRecordById(
            coursePublishdbInfo.getKeySpace(), coursePublishdbInfo.getTableName(), courseId);
    List<Map<String, Object>> resultList =
        (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (!ProjectUtil.CourseMgmtStatus.LIVE
        .getValue()
        .equalsIgnoreCase(additionalCourseInfo.get(JsonKey.STATUS))) {
      if (resultList.isEmpty()) {
        return false;
      }
      Map<String, Object> publishStatus = resultList.get(0);

      if (Status.ACTIVE.getValue() != (Integer) publishStatus.get(JsonKey.STATUS)) {
        return false;
      }
    }
    Boolean flag = false;
    Timestamp ts = new Timestamp(new Date().getTime());
    Map<String, Object> userCourses = new HashMap<>();
    userCourses.put(JsonKey.USER_ID, userId);
    userCourses.put(JsonKey.BATCH_ID, batchId);
    userCourses.put(JsonKey.COURSE_ID, courseId);
    userCourses.put(JsonKey.ID, generatePrimaryKey(userCourses));
    userCourses.put(JsonKey.CONTENT_ID, courseId);
    userCourses.put(JsonKey.COURSE_ENROLL_DATE, ProjectUtil.getFormattedDate());
    userCourses.put(JsonKey.ACTIVE, ProjectUtil.ActiveStatus.ACTIVE.getValue());
    userCourses.put(JsonKey.STATUS, ProjectUtil.ProgressStatus.NOT_STARTED.getValue());
    userCourses.put(JsonKey.DATE_TIME, ts);
    userCourses.put(JsonKey.COURSE_PROGRESS, 0);
    userCourses.put(JsonKey.COURSE_LOGO_URL, additionalCourseInfo.get(JsonKey.COURSE_LOGO_URL));
    userCourses.put(JsonKey.COURSE_NAME, additionalCourseInfo.get(JsonKey.COURSE_NAME));
    userCourses.put(JsonKey.DESCRIPTION, additionalCourseInfo.get(JsonKey.DESCRIPTION));
    if (!StringUtils.isBlank(additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT))) {
      userCourses.put(
          JsonKey.LEAF_NODE_COUNT,
          Integer.parseInt("" + additionalCourseInfo.get(JsonKey.LEAF_NODE_COUNT)));
    }
    userCourses.put(JsonKey.TOC_URL, additionalCourseInfo.get(JsonKey.TOC_URL));
    try {
      cassandraOperation.insertRecord(
          courseEnrollmentdbInfo.getKeySpace(), courseEnrollmentdbInfo.getTableName(), userCourses);
      // TODO: for some reason, ES indexing is failing with Timestamp value. need to
      // check and
      // correct it.
      userCourses.put(JsonKey.DATE_TIME, ProjectUtil.formatDate(ts));
      insertUserCoursesToES(userCourses);
      flag = true;
      Map<String, Object> targetObject =
          TelemetryUtil.generateTargetObject(userId, JsonKey.USER, JsonKey.UPDATE, null);
      List<Map<String, Object>> correlatedObject = new ArrayList<>();
      TelemetryUtil.generateCorrelatedObject(batchId, JsonKey.BATCH, null, correlatedObject);
      TelemetryUtil.telemetryProcessingCall(userCourses, targetObject, correlatedObject);
    } catch (Exception ex) {
      ProjectLogger.log("INSERT RECORD TO USER COURSES EXCEPTION ", ex);
      flag = false;
    }
    return flag;
  }

  private void insertUserCoursesToES(Map<String, Object> courseMap) {
    Request request = new Request();
    request.setOperation(ActorOperations.INSERT_USR_COURSES_INFO_ELASTIC.getValue());
    request.getRequest().put(JsonKey.USER_COURSES, courseMap);
    try {
      tellToAnother(request);
    } catch (Exception ex) {
      ProjectLogger.log("Exception Occurred during saving user count to Es : ", ex);
    }
  }

  @SuppressWarnings("unchecked")
  private String validateBatchInfo(Response courseBatchResult) {
    // check batch exist in db or not
    List<Map<String, Object>> courseList =
        (List<Map<String, Object>>) courseBatchResult.get(JsonKey.RESPONSE);
    if ((courseList.isEmpty())) {
      return ResponseCode.invalidCourseBatchId.getErrorMessage();
    }
    Map<String, Object> courseBatchObject = courseList.get(0);
    // check whether coursebbatch type is invite only or not ...
    if (ProjectUtil.isNull(courseBatchObject.get(JsonKey.ENROLLMENT_TYPE))
        || !((String) courseBatchObject.get(JsonKey.ENROLLMENT_TYPE))
            .equalsIgnoreCase(JsonKey.INVITE_ONLY)) {
      return ResponseCode.enrollmentTypeValidation.getErrorMessage();
    }
    if (ProjectUtil.isNull(courseBatchObject.get(JsonKey.COURSE_CREATED_FOR))
        || ((List) courseBatchObject.get(JsonKey.COURSE_CREATED_FOR)).isEmpty()) {
      return ResponseCode.courseCreatedForIsNull.getErrorMessage();
    }
    return JsonKey.SUCCESS;
  }

  private void processOrgInfo(
      CopyOnWriteArrayList<Map<String, Object>> jsonList, Map<String, Object> dataMap) {

    Map<String, String> channelToRootOrgCache = new HashMap<>();
    List<Map<String, Object>> successList = new ArrayList<>();
    List<Map<String, Object>> failureList = new ArrayList<>();
    // Iteration for rootorg
    for (Map<String, Object> map : jsonList) {
      try {
        if (map.containsKey(JsonKey.IS_ROOT_ORG) && isNotNull(map.get(JsonKey.IS_ROOT_ORG))) {
          Boolean isRootOrg = Boolean.valueOf((String) map.get(JsonKey.IS_ROOT_ORG));
          if (isRootOrg) {
            processOrg(map, dataMap, successList, failureList, channelToRootOrgCache);
            jsonList.remove(map);
          }
        }
      } catch (Exception ex) {
        ProjectLogger.log("Exception ", ex);
        map.put(JsonKey.ERROR_MSG, ex.getMessage());
        failureList.add(map);
      }
    }

    // Iteration for non root org
    for (Map<String, Object> map : jsonList) {
      try {
        processOrg(map, dataMap, successList, failureList, channelToRootOrgCache);
      } catch (Exception ex) {
        ProjectLogger.log("Exception occurs  ", ex);
        map.put(JsonKey.ERROR_MSG, ex.getMessage());
        failureList.add(map);
      }
    }

    dataMap.put(JsonKey.SUCCESS_RESULT, ProjectUtil.convertMapToJsonString(successList));
    dataMap.put(JsonKey.FAILURE_RESULT, ProjectUtil.convertMapToJsonString(failureList));
    dataMap.put(JsonKey.STATUS, BulkProcessStatus.COMPLETED.getValue());

    cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), dataMap);
  }

  @SuppressWarnings("unchecked")
  private void processOrg(
      Map<String, Object> map,
      Map<String, Object> dataMap,
      List<Map<String, Object>> successList,
      List<Map<String, Object>> failureList,
      Map<String, String> channelToRootOrgCache) {

    Map<String, Object> concurrentHashMap = map;
    Util.DbInfo orgDbInfo = Util.dbInfoMap.get(JsonKey.ORG_DB);
    Object[] orgContactList = null;
    String contactDetails = null;
    boolean isOrgUpdated = false;
    // validate location code

    if (concurrentHashMap.containsKey(JsonKey.LOCATION_CODE)
        && StringUtils.isNotBlank((String) concurrentHashMap.get(JsonKey.LOCATION_CODE))) {
      try {
        convertCommaSepStringToList(concurrentHashMap, JsonKey.LOCATION_CODE);
        List<String> locationIdList =
            validator.getValidatedLocationIds(
                getActorRef(LocationActorOperation.SEARCH_LOCATION.getValue()),
                (List<String>) concurrentHashMap.get(JsonKey.LOCATION_CODE));
        concurrentHashMap.put(JsonKey.LOCATION_IDS, locationIdList);
        concurrentHashMap.remove(JsonKey.LOCATION_CODE);
      } catch (Exception ex) {
        concurrentHashMap.put(JsonKey.ERROR_MSG, "Invalid value for LocationCode.");
        failureList.add(concurrentHashMap);
        return;
      }
    }

    if (concurrentHashMap.containsKey(JsonKey.ORG_TYPE)
        && !ProjectUtil.isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.ORG_TYPE))) {
      String orgTypeId = validateOrgType((String) concurrentHashMap.get(JsonKey.ORG_TYPE));
      if (null == orgTypeId) {
        concurrentHashMap.put(JsonKey.ERROR_MSG, "Invalid OrgType.");
        failureList.add(concurrentHashMap);
        return;
      } else {
        concurrentHashMap.put(JsonKey.ORG_TYPE_ID, orgTypeId);
      }
    }

    if (concurrentHashMap.containsKey(JsonKey.LOC_ID)
        && !ProjectUtil.isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.LOC_ID))) {
      String locId = validateLocationId((String) concurrentHashMap.get(JsonKey.LOC_ID));
      if (null == locId) {
        concurrentHashMap.put(JsonKey.ERROR_MSG, "Invalid Location Id.");
        failureList.add(concurrentHashMap);
        return;
      } else {
        concurrentHashMap.put(JsonKey.LOC_ID, locId);
      }
    }

    if (isNull(concurrentHashMap.get(JsonKey.ORGANISATION_NAME))
        || ProjectUtil.isStringNullOREmpty(
            (String) concurrentHashMap.get(JsonKey.ORGANISATION_NAME))) {
      ProjectLogger.log("orgName is mandatory for org creation.");
      concurrentHashMap.put(JsonKey.ERROR_MSG, "orgName is mandatory for org creation.");
      failureList.add(concurrentHashMap);
      return;
    }

    Boolean isRootOrg;
    if (isNotNull(concurrentHashMap.get(JsonKey.IS_ROOT_ORG))) {
      isRootOrg = Boolean.valueOf((String) concurrentHashMap.get(JsonKey.IS_ROOT_ORG));
    } else {
      isRootOrg = false;
    }
    concurrentHashMap.put(JsonKey.IS_ROOT_ORG, isRootOrg);

    if (concurrentHashMap.containsKey(JsonKey.CONTACT_DETAILS)
        && !ProjectUtil.isStringNullOREmpty(
            (String) concurrentHashMap.get(JsonKey.CONTACT_DETAILS))) {

      contactDetails = (String) concurrentHashMap.get(JsonKey.CONTACT_DETAILS);
      contactDetails = contactDetails.replaceAll("'", "\"");
      try {
        orgContactList = mapper.readValue(contactDetails, Object[].class);

      } catch (IOException ex) {
        ProjectLogger.log("Unable to parse Org contact Details - OrgBulkUpload.", ex);
        concurrentHashMap.put(
            JsonKey.ERROR_MSG, "Unable to parse Org contact Details - OrgBulkUpload.");
        failureList.add(concurrentHashMap);
        return;
      }
    }

    if (isNotNull(concurrentHashMap.get(JsonKey.PROVIDER))
        || isNotNull(concurrentHashMap.get(JsonKey.EXTERNAL_ID))) {
      if (isNull(concurrentHashMap.get(JsonKey.PROVIDER))
          || isNull(concurrentHashMap.get(JsonKey.EXTERNAL_ID))) {
        ProjectLogger.log("Provider and external ids both should exist.");
        concurrentHashMap.put(JsonKey.ERROR_MSG, "Provider and external ids both should exist.");
        failureList.add(concurrentHashMap);
        return;
      }

      Map<String, Object> dbMap = new HashMap<>();
      dbMap.put(JsonKey.PROVIDER, concurrentHashMap.get(JsonKey.PROVIDER));
      dbMap.put(JsonKey.EXTERNAL_ID, concurrentHashMap.get(JsonKey.EXTERNAL_ID));
      Response result =
          cassandraOperation.getRecordsByProperties(
              orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), dbMap);
      List<Map<String, Object>> list = (List<Map<String, Object>>) result.get(JsonKey.RESPONSE);
      if (!(list.isEmpty())) {

        Map<String, Object> orgResult = list.get(0);

        boolean dbRootOrg = (boolean) orgResult.get(JsonKey.IS_ROOT_ORG);
        if (isRootOrg != dbRootOrg) {
          ProjectLogger.log("Can not update isRootorg value ");
          concurrentHashMap.put(JsonKey.ERROR_MSG, "Can not update isRootorg value ");
          failureList.add(concurrentHashMap);
          return;
        }

        if (!compareStrings(
            (String) concurrentHashMap.get(JsonKey.CHANNEL),
            (String) orgResult.get(JsonKey.CHANNEL))) {
          ProjectLogger.log("Can not update is Channel value ");
          concurrentHashMap.put(JsonKey.ERROR_MSG, "Can not update channel value ");
          failureList.add(concurrentHashMap);
          return;
        }

        // check logic here to check the hashtag id ... if in db null and requested one
        // not exist in
        // db only then we are going to update the new one...

        if (!ProjectUtil.isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.HASHTAGID))) {
          String requestedHashTagId = (String) concurrentHashMap.get(JsonKey.HASHTAGID);
          // if both are not equal ...
          if (!requestedHashTagId.equals(orgResult.get(JsonKey.HASHTAGID))) {
            Map<String, Object> dbMap1 = new HashMap<>();
            dbMap1.put(JsonKey.HASHTAGID, concurrentHashMap.get(JsonKey.HASHTAGID));
            Response result1 =
                cassandraOperation.getRecordsByProperties(
                    orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), dbMap1);
            List<Map<String, Object>> list1 =
                (List<Map<String, Object>>) result1.get(JsonKey.RESPONSE);
            if (!list1.isEmpty()) {
              ProjectLogger.log("Can not update hashtag value , since it is already exist ");
              concurrentHashMap.put(
                  JsonKey.ERROR_MSG, "Hash Tag ID already exist for another org ");
              failureList.add(concurrentHashMap);
              return;
            }
          }
        }
        concurrentHashMap.put(JsonKey.ID, orgResult.get(JsonKey.ID));

        try {
          // This will remove all extra unnecessary parameter from request
          Organization org = mapper.convertValue(concurrentHashMap, Organization.class);
          concurrentHashMap = mapper.convertValue(org, Map.class);
          cassandraOperation.upsertRecord(
              orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), concurrentHashMap);
          Response orgResponse = new Response();

          // sending the org contact as List if it is null simply remove from map
          if (isNotNull(orgContactList)) {
            concurrentHashMap.put(JsonKey.CONTACT_DETAILS, Arrays.asList(orgContactList));
          }

          orgResponse.put(JsonKey.ORGANISATION, concurrentHashMap);
          orgResponse.put(JsonKey.OPERATION, ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
          ProjectLogger.log(
              "Calling background job to save org data into ES" + orgResult.get(JsonKey.ID));
          Request request = new Request();
          request.setOperation(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
          request.getRequest().put(JsonKey.ORGANISATION, concurrentHashMap);
          tellToAnother(request);
          successList.add(concurrentHashMap);
          // process Audit Log
          processAuditLog(
              concurrentHashMap, ActorOperations.UPDATE_ORG.getValue(), "", JsonKey.ORGANISATION);
          // TODO: create telemetry for update org
          isOrgUpdated = true;
          generateTelemetryForOrganisation(
              concurrentHashMap, (String) orgResult.get(JsonKey.ID), isOrgUpdated);
          return;
        } catch (Exception ex) {

          ProjectLogger.log("Exception occurs  ", ex);
          concurrentHashMap.put(JsonKey.ERROR_MSG, ex.getMessage());
          failureList.add(concurrentHashMap);
          return;
        }
      }
    }

    if (isRootOrg) {
      if (isNull(concurrentHashMap.get(JsonKey.CHANNEL))) {
        concurrentHashMap.put(JsonKey.ERROR_MSG, "Channel is mandatory for root org ");
        failureList.add(concurrentHashMap);
        return;
      }
      // check for unique root org for channel -----
      List<Map<String, Object>> rootOrgListRes =
          searchOrgByChannel((String) concurrentHashMap.get(JsonKey.CHANNEL), orgDbInfo);
      // if for root org true for this channel means simply update the existing record
      // ...
      if (CollectionUtils.isNotEmpty(rootOrgListRes)) {
        Map<String, Object> rootOrgInfo = rootOrgListRes.get(0);
        concurrentHashMap.put(JsonKey.ID, rootOrgInfo.get(JsonKey.ID));
        concurrentHashMap.put(JsonKey.UPDATED_BY, dataMap.get(JsonKey.UPLOADED_BY));
        concurrentHashMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        if (!compareStrings(
            (String) concurrentHashMap.get(JsonKey.EXTERNAL_ID),
            (String) rootOrgInfo.get(JsonKey.EXTERNAL_ID))) {
          ProjectLogger.log("Can not update is External Id ");
          concurrentHashMap.put(JsonKey.ERROR_MSG, "Can not update External Id ");
          failureList.add(concurrentHashMap);
          return;
        }

        if (!compareStrings(
            (String) concurrentHashMap.get(JsonKey.PROVIDER),
            (String) rootOrgInfo.get(JsonKey.PROVIDER))) {
          ProjectLogger.log("Can not update is Provider ");
          concurrentHashMap.put(JsonKey.ERROR_MSG, "Can not update Provider ");
          failureList.add(concurrentHashMap);
          return;
        }

        // check for duplicate hashtag id ...
        if (!ProjectUtil.isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.HASHTAGID))) {
          String requestedHashTagId = (String) concurrentHashMap.get(JsonKey.HASHTAGID);
          // if both are not equal ...
          if (!requestedHashTagId.equalsIgnoreCase((String) rootOrgInfo.get(JsonKey.HASHTAGID))) {
            Map<String, Object> dbMap1 = new HashMap<>();
            dbMap1.put(JsonKey.HASHTAGID, concurrentHashMap.get(JsonKey.HASHTAGID));
            Response result1 =
                cassandraOperation.getRecordsByProperties(
                    orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), dbMap1);
            List<Map<String, Object>> list1 =
                (List<Map<String, Object>>) result1.get(JsonKey.RESPONSE);
            if (!list1.isEmpty()) {
              ProjectLogger.log("Can not update hashtag value , since it is already exist ");
              concurrentHashMap.put(
                  JsonKey.ERROR_MSG, "Hash Tag ID already exist for another org ");
              failureList.add(concurrentHashMap);
              return;
            }
          }
        }

        try {
          // This will remove all extra unnecessary parameter from request
          Organization org = mapper.convertValue(concurrentHashMap, Organization.class);
          concurrentHashMap = mapper.convertValue(org, Map.class);
          cassandraOperation.upsertRecord(
              orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), concurrentHashMap);
          Response orgResponse = new Response();

          // sending the org contact as List if it is null simply remove from map
          if (isNotNull(orgContactList)) {
            concurrentHashMap.put(JsonKey.CONTACT_DETAILS, Arrays.asList(orgContactList));
          }

          orgResponse.put(JsonKey.ORGANISATION, concurrentHashMap);
          orgResponse.put(JsonKey.OPERATION, ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
          ProjectLogger.log(
              "Calling background job to save org data into ES" + rootOrgInfo.get(JsonKey.ID));
          Request request = new Request();
          request.setOperation(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
          request.getRequest().put(JsonKey.ORGANISATION, concurrentHashMap);
          tellToAnother(request);
          successList.add(concurrentHashMap);
          // process Audit Log
          processAuditLog(
              concurrentHashMap, ActorOperations.UPDATE_ORG.getValue(), "", JsonKey.ORGANISATION);
          // TODO: create telemetry for update org
          isOrgUpdated = true;
          generateTelemetryForOrganisation(
              concurrentHashMap, (String) concurrentHashMap.get(JsonKey.ID), isOrgUpdated);
          return;
        } catch (Exception ex) {

          ProjectLogger.log("Exception occurs  ", ex);
          concurrentHashMap.put(JsonKey.ERROR_MSG, ex.getMessage());
          failureList.add(concurrentHashMap);
          return;
        }
      }

    } else {

      if (concurrentHashMap.containsKey(JsonKey.CHANNEL)
          && !(ProjectUtil.isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.CHANNEL)))) {
        String channel = (String) concurrentHashMap.get(JsonKey.CHANNEL);
        if (channelToRootOrgCache.containsKey(channel)) {
          concurrentHashMap.put(JsonKey.ROOT_ORG_ID, channelToRootOrgCache.get(channel));
        } else {
          // check for unique root org for channel -----
          List<Map<String, Object>> rootOrgListRes =
              searchOrgByChannel((String) concurrentHashMap.get(JsonKey.CHANNEL), orgDbInfo);

          if (CollectionUtils.isNotEmpty(rootOrgListRes)) {

            Map<String, Object> rootOrgResult = rootOrgListRes.get(0);
            concurrentHashMap.put(JsonKey.ROOT_ORG_ID, rootOrgResult);
            channelToRootOrgCache.put(
                (String) concurrentHashMap.get(JsonKey.CHANNEL),
                (String) rootOrgResult.get(JsonKey.ID));

          } else {
            concurrentHashMap.put(
                JsonKey.ERROR_MSG,
                "This is not root org and No Root Org id exist for channel  "
                    + concurrentHashMap.get(JsonKey.CHANNEL));
            failureList.add(concurrentHashMap);
            return;
          }
        }
      } else if (concurrentHashMap.containsKey(JsonKey.PROVIDER)
          && !(StringUtils.isBlank(JsonKey.PROVIDER))) {
        String rootOrgId =
            Util.getRootOrgIdFromChannel((String) concurrentHashMap.get(JsonKey.PROVIDER));

        if (!(StringUtils.isBlank(rootOrgId))) {
          concurrentHashMap.put(JsonKey.ROOT_ORG_ID, rootOrgId);
        } else {
          concurrentHashMap.put(JsonKey.ROOT_ORG_ID, JsonKey.DEFAULT_ROOT_ORG_ID);
        }

      } else {
        concurrentHashMap.put(JsonKey.ROOT_ORG_ID, JsonKey.DEFAULT_ROOT_ORG_ID);
      }
    }

    // we can put logic here to check uniqueness of hash tag id in order to create
    // new organisation
    // ...
    if (!StringUtils.isBlank((String) concurrentHashMap.get(JsonKey.HASHTAGID))) {

      Map<String, Object> dbMap1 = new HashMap<>();
      dbMap1.put(JsonKey.HASHTAGID, concurrentHashMap.get(JsonKey.HASHTAGID));
      Response result1 =
          cassandraOperation.getRecordsByProperties(
              orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), dbMap1);
      List<Map<String, Object>> list1 = (List<Map<String, Object>>) result1.get(JsonKey.RESPONSE);
      if (!list1.isEmpty()) {
        ProjectLogger.log("Can not update hashtag value , since it is already exist ");
        concurrentHashMap.put(JsonKey.ERROR_MSG, "Hash Tag ID already exist for another org ");
        failureList.add(concurrentHashMap);
        return;
      }
    }

    concurrentHashMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    concurrentHashMap.put(JsonKey.STATUS, ProjectUtil.OrgStatus.ACTIVE.getValue());
    // allow lower case values for provider and externalId to the database
    if (concurrentHashMap.get(JsonKey.PROVIDER) != null) {
      concurrentHashMap.put(
          JsonKey.PROVIDER, ((String) concurrentHashMap.get(JsonKey.PROVIDER)).toLowerCase());
    }
    if (concurrentHashMap.get(JsonKey.EXTERNAL_ID) != null) {
      concurrentHashMap.put(
          JsonKey.EXTERNAL_ID, ((String) concurrentHashMap.get(JsonKey.EXTERNAL_ID)).toLowerCase());
    }
    concurrentHashMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    concurrentHashMap.put(JsonKey.CREATED_BY, dataMap.get(JsonKey.UPLOADED_BY));
    String uniqueId = ProjectUtil.getUniqueIdFromTimestamp(1);
    if (StringUtils.isBlank((String) concurrentHashMap.get(JsonKey.ID))) {
      concurrentHashMap.put(JsonKey.ID, uniqueId);
      // if user does not provide hash tag id in the file set it to equivalent to org
      // id
      if (ProjectUtil.isStringNullOREmpty((String) concurrentHashMap.get(JsonKey.HASHTAGID))) {
        concurrentHashMap.put(JsonKey.HASHTAGID, uniqueId);
      }
      if (ProjectUtil.isNull(concurrentHashMap.get(JsonKey.IS_ROOT_ORG))) {
        concurrentHashMap.put(JsonKey.IS_ROOT_ORG, false);
      }
      Boolean isRootOrgFlag = (Boolean) concurrentHashMap.get(JsonKey.IS_ROOT_ORG);

      // Remove the slug key if coming form user input.
      concurrentHashMap.remove(JsonKey.SLUG);
      if (concurrentHashMap.containsKey(JsonKey.CHANNEL)) {
        String slug =
            Slug.makeSlug((String) concurrentHashMap.getOrDefault(JsonKey.CHANNEL, ""), true);
        if (null != isRootOrgFlag && isRootOrgFlag) {
          boolean bool = isSlugUnique(slug);
          if (bool) {
            concurrentHashMap.put(JsonKey.SLUG, slug);
          } else {
            ProjectLogger.log(ResponseCode.slugIsNotUnique.getErrorMessage());
            concurrentHashMap.put(
                JsonKey.ERROR_MSG, ResponseCode.slugIsNotUnique.getErrorMessage());
            failureList.add(concurrentHashMap);
            return;
          }
        } else {
          concurrentHashMap.put(JsonKey.SLUG, slug);
        }
      }

      if (null != isRootOrgFlag && isRootOrgFlag) {
        boolean bool = Util.registerChannel(concurrentHashMap);
        if (!bool) {
          ProjectLogger.log("channel registration failed.");
          concurrentHashMap.put(JsonKey.ERROR_MSG, "channel registration failed.");
          failureList.add(concurrentHashMap);
          return;
        }
      }

      if (null != isRootOrgFlag && isRootOrgFlag) {
        concurrentHashMap.put(JsonKey.ROOT_ORG_ID, uniqueId);
      }
    }

    concurrentHashMap.put(JsonKey.CONTACT_DETAILS, contactDetails);

    try {
      // This will remove all extra unnecessary parameter from request
      Organization org = mapper.convertValue(concurrentHashMap, Organization.class);
      concurrentHashMap = mapper.convertValue(org, Map.class);
      cassandraOperation.upsertRecord(
          orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), concurrentHashMap);
      Response orgResponse = new Response();

      // sending the org contact as List if it is null simply remove from map
      if (isNotNull(orgContactList)) {
        concurrentHashMap.put(JsonKey.CONTACT_DETAILS, Arrays.asList(orgContactList));
      }
      orgResponse.put(JsonKey.ORGANISATION, concurrentHashMap);
      orgResponse.put(JsonKey.OPERATION, ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
      ProjectLogger.log("Calling background job to save org data into ES" + uniqueId);
      Request request = new Request();
      request.setOperation(ActorOperations.INSERT_ORG_INFO_ELASTIC.getValue());
      request.getRequest().put(JsonKey.ORGANISATION, concurrentHashMap);
      tellToAnother(request);
      successList.add(concurrentHashMap);
      // process Audit Log
      processAuditLog(
          concurrentHashMap, ActorOperations.CREATE_ORG.getValue(), "", JsonKey.ORGANISATION);
    } catch (Exception ex) {

      ProjectLogger.log("Exception occurs  ", ex);
      concurrentHashMap.put(JsonKey.ERROR_MSG, ex.getMessage());
      failureList.add(concurrentHashMap);
      return;
    }
    generateTelemetryForOrganisation(map, uniqueId, isOrgUpdated);
  }

  private List<Map<String, Object>> searchOrgByChannel(String channel, Util.DbInfo orgDbInfo) {
    Map<String, Object> filters = new HashMap<>();
    filters.put(JsonKey.CHANNEL, channel);
    filters.put(JsonKey.IS_ROOT_ORG, true);

    Response orgResponse =
        cassandraOperation.getRecordsByProperties(
            orgDbInfo.getKeySpace(), orgDbInfo.getTableName(), filters);
    List<Map<String, Object>> rootOrgListRes =
        (List<Map<String, Object>>) orgResponse.get(JsonKey.RESPONSE);
    return rootOrgListRes;
  }

  private void generateTelemetryForOrganisation(
      Map<String, Object> map, String id, boolean isOrgUpdated) {

    String orgState = JsonKey.CREATE;
    if (isOrgUpdated) {
      orgState = JsonKey.UPDATE;
    }
    Map<String, Object> targetObject =
        TelemetryUtil.generateTargetObject(id, JsonKey.ORGANISATION, orgState, null);
    List<Map<String, Object>> correlatedObject = new ArrayList<>();
    TelemetryUtil.generateCorrelatedObject(id, JsonKey.ORGANISATION, null, correlatedObject);
    TelemetryUtil.telemetryProcessingCall(map, targetObject, correlatedObject);
  }

  private String validateLocationId(String locId) {
    String locnId = null;
    try {
      if (locnIdList.isEmpty()) {
        Util.DbInfo geoLocDbInfo = Util.dbInfoMap.get(JsonKey.GEO_LOCATION_DB);
        Response response =
            cassandraOperation.getAllRecords(
                geoLocDbInfo.getKeySpace(), geoLocDbInfo.getTableName());
        List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        if (!list.isEmpty()) {
          for (Map<String, Object> map : list) {
            locnIdList.add(((String) map.get(JsonKey.ID)));
          }
        }
      }
      if (locnIdList.contains(locId)) {
        return locId;
      } else {
        return null;
      }
    } catch (Exception ex) {
      ProjectLogger.log("Exception occurred while validating location id ", ex);
    }
    return locnId;
  }

  private String validateOrgType(String orgType) {
    String orgTypeId = null;
    try {
      if (!StringUtils.isBlank(DataCacheHandler.getOrgTypeMap().get(orgType.toLowerCase()))) {
        orgTypeId = DataCacheHandler.getOrgTypeMap().get(orgType.toLowerCase());
      } else {
        Util.DbInfo orgTypeDbInfo = Util.dbInfoMap.get(JsonKey.ORG_TYPE_DB);
        Response response =
            cassandraOperation.getAllRecords(
                orgTypeDbInfo.getKeySpace(), orgTypeDbInfo.getTableName());
        List<Map<String, Object>> list = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        if (!list.isEmpty()) {
          for (Map<String, Object> map : list) {
            if ((((String) map.get(JsonKey.NAME)).toLowerCase())
                .equalsIgnoreCase(orgType.toLowerCase())) {
              orgTypeId = (String) map.get(JsonKey.ID);
              DataCacheHandler.getOrgTypeMap()
                  .put(
                      ((String) map.get(JsonKey.NAME)).toLowerCase(), (String) map.get(JsonKey.ID));
            }
          }
        }
      }
    } catch (Exception ex) {
      ProjectLogger.log("Exception occurred while getting orgTypeId from OrgType", ex);
    }
    return orgTypeId;
  }

  private Map<String, Object> elasticSearchComplexSearch(
      Map<String, Object> filters, String index, String type) {

    SearchDTO searchDTO = new SearchDTO();
    searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);

    return ElasticSearchUtil.complexSearch(searchDTO, index, type);
  }

  private void processUserInfo(
      List<Map<String, Object>> dataMapList, String processId, String updatedBy) {
    // update status from NEW to INProgress
    updateStatusForProcessing(processId);
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    List<Map<String, Object>> failureUserReq = new ArrayList<>();
    List<Map<String, Object>> successUserReq = new ArrayList<>();
    Map<String, Object> userMap = null;
    /*
     * To store hashTagId inside user_org table, first we need to get hashTagId from
     * provided organisation ID. Currently in bulk user upload, we are passing only
     * one organisation, so we can get the details before for loop and reuse it.
     */
    String hashTagId = null;
    if (dataMapList != null && dataMapList.size() > 0) {
      String orgId = (String) dataMapList.get(0).get(JsonKey.ORGANISATION_ID);
      if (StringUtils.isNotBlank(orgId)) {
        Map<String, Object> map =
            ElasticSearchUtil.getDataByIdentifier(
                ProjectUtil.EsIndex.sunbird.getIndexName(),
                ProjectUtil.EsType.organisation.getTypeName(),
                orgId);
        if (MapUtils.isNotEmpty(map)) {
          hashTagId = (String) map.get(JsonKey.HASHTAGID);
          ProjectLogger.log(
              "BulkUploadBackGroundJobActor:processUserInfo org hashTagId value : " + hashTagId,
              LoggerEnum.INFO.name());
        }
      }
    }
    for (int i = 0; i < dataMapList.size(); i++) {
      userMap = dataMapList.get(i);
      Map<String, Object> welcomeMailTemplateMap = new HashMap<>();
      if (StringUtils.isBlank((String) userMap.get(JsonKey.PASSWORD))) {
        String randomPassword = ProjectUtil.generateRandomPassword();
        userMap.put(JsonKey.PASSWORD, randomPassword);
        welcomeMailTemplateMap.put(JsonKey.TEMPORARY_PASSWORD, randomPassword);
      } else {
        welcomeMailTemplateMap.put(JsonKey.TEMPORARY_PASSWORD, userMap.get(JsonKey.PASSWORD));
      }
      String errMsg = validateUser(userMap);
      if (errMsg.equalsIgnoreCase(JsonKey.SUCCESS)) {
        try {

          // convert userName,provide,loginId,externalId.. value to lowercase
          updateMapSomeValueTOLowerCase(userMap);
          Map<String, Object> foundUserMap = findUser(userMap);
          foundUserMap = insertRecordToKeyCloak(userMap, foundUserMap, updatedBy);
          Map<String, Object> tempMap = new HashMap<>();
          tempMap.putAll(userMap);
          tempMap.remove(JsonKey.EMAIL_VERIFIED);
          tempMap.remove(JsonKey.POSITION);
          // remove externalID and Provider as we are not saving these to user table
          tempMap.remove(JsonKey.EXTERNAL_ID);
          tempMap.remove(JsonKey.EXTERNAL_ID_PROVIDER);
          tempMap.remove(JsonKey.EXTERNAL_ID_TYPE);
          tempMap.remove(JsonKey.ORGANISATION_ID);
          tempMap.put(JsonKey.EMAIL_VERIFIED, false);
          Response response = null;
          if (null == tempMap.get(JsonKey.OPERATION)) {
            // will allowing only PUBLIC role at user level.
            tempMap.remove(JsonKey.ROLES);
            // insert user record
            // Add only PUBLIC role to user
            List<String> list = new ArrayList<>();
            list.add(ProjectUtil.UserRole.PUBLIC.getValue());
            tempMap.put(JsonKey.ROLES, list);
            try {
              UserUtility.encryptUserData(tempMap);
            } catch (Exception ex) {
              ProjectLogger.log(
                  "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor during data encryption :",
                  ex);
              throw new ProjectCommonException(
                  ResponseCode.userDataEncryptionError.getErrorCode(),
                  ResponseCode.userDataEncryptionError.getErrorMessage(),
                  ResponseCode.SERVER_ERROR.getResponseCode());
            }
            tempMap.put(JsonKey.CREATED_BY, updatedBy);
            tempMap.put(JsonKey.IS_DELETED, false);
            tempMap.remove(JsonKey.EXTERNAL_IDS);
            try {
              response =
                  cassandraOperation.insertRecord(
                      usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), tempMap);
              // insert details to user_org table
              userMap.put(JsonKey.HASHTAGID, hashTagId);
              Util.registerUserToOrg(userMap);
              // removing added hashTagId
              userMap.remove(JsonKey.HASHTAGID);
            } catch (Exception ex) {
              // incase of exception also removing added hashTagId
              userMap.remove(JsonKey.HASHTAGID);
              ProjectLogger.log(
                  "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor:", ex);
              userMap.remove(JsonKey.ID);
              userMap.remove(JsonKey.PASSWORD);
              userMap.put(JsonKey.ERROR_MSG, ex.getMessage() + " ,user insertion failed.");
              removeOriginalExternalIds(
                  (List<Map<String, Object>>) userMap.get(JsonKey.EXTERNAL_IDS));
              failureUserReq.add(userMap);
              continue;
            } finally {
              if (null == response) {
                ssoManager.removeUser(userMap);
              }
            }
            // send the welcome mail to user
            welcomeMailTemplateMap.putAll(userMap);
            // the loginid will become user id for logon purpose .
            welcomeMailTemplateMap.put(JsonKey.USERNAME, userMap.get(JsonKey.LOGIN_ID));
            Request welcomeMailReqObj = Util.sendOnboardingMail(welcomeMailTemplateMap);
            if (null != welcomeMailReqObj) {
              tellToAnother(welcomeMailReqObj);
            }

            if (StringUtils.isNotBlank((String) userMap.get(JsonKey.PHONE))) {
              Util.sendSMS(userMap);
            }
            // process Audit Log
            processAuditLog(
                userMap, ActorOperations.CREATE_USER.getValue(), updatedBy, JsonKey.USER);
            // generate telemetry for new user creation
            // object of telemetry event...
            Map<String, Object> targetObject = null;
            List<Map<String, Object>> correlatedObject = new ArrayList<>();

            targetObject =
                TelemetryUtil.generateTargetObject(
                    (String) userMap.get(JsonKey.ID), JsonKey.USER, JsonKey.CREATE, null);
            TelemetryUtil.telemetryProcessingCall(userMap, targetObject, correlatedObject);
          } else {
            // update user record
            tempMap.put(JsonKey.UPDATED_BY, updatedBy);
            tempMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
            try {
              UserUtility.encryptUserData(tempMap);
            } catch (Exception ex) {
              ProjectLogger.log(
                  "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor during data encryption :",
                  ex);
              throw new ProjectCommonException(
                  ResponseCode.userDataEncryptionError.getErrorCode(),
                  ResponseCode.userDataEncryptionError.getErrorMessage(),
                  ResponseCode.SERVER_ERROR.getResponseCode());
            }
            try {
              removeFieldsFrmUpdateReq(tempMap);
              response =
                  cassandraOperation.updateRecord(
                      usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), tempMap);
              // update user-org table(role update)
              userMap.put(JsonKey.UPDATED_BY, updatedBy);
              userMap.put(JsonKey.HASHTAGID, hashTagId);
              Util.upsertUserOrgData(userMap);
              userMap.remove(JsonKey.HASHTAGID);
            } catch (Exception ex) {
              userMap.remove(JsonKey.HASHTAGID);
              ProjectLogger.log(
                  "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor:", ex);
              userMap.remove(JsonKey.ID);
              userMap.remove(JsonKey.PASSWORD);
              userMap.put(JsonKey.ERROR_MSG, ex.getMessage() + " ,user updation failed.");
              removeOriginalExternalIds(
                  (List<Map<String, Object>>) userMap.get(JsonKey.EXTERNAL_IDS));
              failureUserReq.add(userMap);
              continue;
            }
            // Process Audit Log
            processAuditLog(
                userMap, ActorOperations.UPDATE_USER.getValue(), updatedBy, JsonKey.USER);
          }

          // update the user external identity data
          try {
            if (null != userMap.get(JsonKey.EXTERNAL_IDS)) {
              Util.updateUserExtId(userMap);
              removeOriginalExternalIds(
                  (List<Map<String, Object>>) userMap.get(JsonKey.EXTERNAL_IDS));
            }
          } catch (Exception ex) {
            userMap.put(
                JsonKey.ERROR_MSG, "Update of user external IDs failed. " + ex.getMessage());
          }
          // save successfully created user data
          tempMap.putAll(userMap);
          tempMap.remove(JsonKey.STATUS);
          tempMap.remove(JsonKey.CREATED_DATE);
          tempMap.remove(JsonKey.CREATED_BY);
          tempMap.remove(JsonKey.ID);
          tempMap.remove(JsonKey.LOGIN_ID);
          tempMap.put(JsonKey.PASSWORD, "*****");
          successUserReq.add(tempMap);

          // update elastic search
          ProjectLogger.log(
              "making a call to save user data to ES in BulkUploadBackGroundJobActor");
          Request request = new Request();
          request.setOperation(ActorOperations.UPDATE_USER_INFO_ELASTIC.getValue());
          request.getRequest().put(JsonKey.ID, userMap.get(JsonKey.ID));
          tellToAnother(request);
          // generate telemetry for update user
          // object of telemetry event...
          Map<String, Object> targetObject = null;
          List<Map<String, Object>> correlatedObject = new ArrayList<>();
          targetObject =
              TelemetryUtil.generateTargetObject(
                  (String) userMap.get(JsonKey.ID), JsonKey.USER, JsonKey.UPDATE, null);
          TelemetryUtil.telemetryProcessingCall(userMap, targetObject, correlatedObject);
        } catch (Exception ex) {
          ProjectLogger.log(
              "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor:", ex);
          userMap.remove(JsonKey.ID);
          userMap.remove(JsonKey.PASSWORD);
          userMap.put(JsonKey.ERROR_MSG, ex.getMessage());
          removeOriginalExternalIds((List<Map<String, Object>>) userMap.get(JsonKey.EXTERNAL_IDS));
          failureUserReq.add(userMap);
        }
      } else {
        userMap.put(JsonKey.ERROR_MSG, errMsg);
        removeOriginalExternalIds((List<Map<String, Object>>) userMap.get(JsonKey.EXTERNAL_IDS));
        failureUserReq.add(userMap);
      }
    }
    // Insert record to BulkDb table
    // After Successful completion of bulk upload process , encrypt the success and
    // failure result
    // and delete the user data(csv file data)
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.ID, processId);
    try {
      map.put(
          JsonKey.SUCCESS_RESULT,
          UserUtility.encryptData(ProjectUtil.convertMapToJsonString(successUserReq)));
      map.put(
          JsonKey.FAILURE_RESULT,
          UserUtility.encryptData(ProjectUtil.convertMapToJsonString(failureUserReq)));
    } catch (Exception e1) {
      ProjectLogger.log(
          "Exception occurred while encrypting success and failure result in bulk upload process : ",
          e1);
    }
    map.put(JsonKey.PROCESS_END_TIME, ProjectUtil.getFormattedDate());
    map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.COMPLETED.getValue());
    map.put(JsonKey.DATA, "");
    try {
      cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
    } catch (Exception e) {
      ProjectLogger.log(
          "Exception Occurred while updating bulk_upload_process in BulkUploadBackGroundJobActor : ",
          e);
    }
  }

  private void removeOriginalExternalIds(List<Map<String, Object>> externalIds) {
    externalIds.forEach(
        externalId -> {
          externalId.put(JsonKey.ID, externalId.get(JsonKey.ORIGINAL_EXTERNAL_ID));
          externalId.remove(JsonKey.ORIGINAL_EXTERNAL_ID);
          externalId.put(JsonKey.PROVIDER, externalId.get(JsonKey.ORIGINAL_PROVIDER));
          externalId.remove(JsonKey.ORIGINAL_PROVIDER);
          externalId.put(JsonKey.ID_TYPE, externalId.get(JsonKey.ORIGINAL_ID_TYPE));
          externalId.remove(JsonKey.ORIGINAL_ID_TYPE);
        });
  }

  private void parseExternalIds(Map<String, Object> userMap) throws IOException {
    if (userMap.containsKey(JsonKey.EXTERNAL_IDS)
        && StringUtils.isNotBlank((String) userMap.get(JsonKey.EXTERNAL_IDS))) {
      String externalIds = (String) userMap.get(JsonKey.EXTERNAL_IDS);
      List<Map<String, String>> externalIdList = new ArrayList<>();
      externalIdList = mapper.readValue(externalIds, List.class);
      userMap.put(JsonKey.EXTERNAL_IDS, externalIdList);
    }
  }

  private void convertCommaSepStringToList(Map<String, Object> map, String property) {
    String[] props = ((String) map.get(property)).split(",");
    List<String> list = new ArrayList<>(Arrays.asList(props));
    map.put(property, list);
  }

  private void processAuditLog(
      Map<String, Object> dataMap, String actorOperationType, String updatedBy, String objectType) {
    Request req = new Request();
    Response res = new Response();
    req.setRequestId(processId);
    req.setOperation(actorOperationType);
    dataMap.remove("header");
    req.getRequest().put(JsonKey.REQUESTED_BY, updatedBy);
    if (objectType.equalsIgnoreCase(JsonKey.USER)) {
      req.getRequest().put(JsonKey.USER, dataMap);
      res.getResult().put(JsonKey.USER_ID, dataMap.get(JsonKey.USER_ID));
    } else if (objectType.equalsIgnoreCase(JsonKey.ORGANISATION)) {
      req.getRequest().put(JsonKey.ORGANISATION, dataMap);
      res.getResult().put(JsonKey.ORGANISATION_ID, dataMap.get(JsonKey.ID));
    } else if (objectType.equalsIgnoreCase(JsonKey.BATCH)) {
      req.getRequest().put(JsonKey.BATCH, dataMap);
      res.getResult().put(JsonKey.BATCH_ID, dataMap.get(JsonKey.ID));
    }
    saveAuditLog(res, actorOperationType, req);
  }

  private void updateStatusForProcessing(String processId) {
    // Update status to BulkDb table
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.ID, processId);
    map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.IN_PROGRESS.getValue());
    try {
      cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
    } catch (Exception e) {
      ProjectLogger.log(
          "Exception Occurred while updating bulk_upload_process in BulkUploadBackGroundJobActor : ",
          e);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getBulkData(String processId) {
    try {
      Map<String, Object> map = new HashMap<>();
      map.put(JsonKey.ID, processId);
      map.put(JsonKey.PROCESS_START_TIME, ProjectUtil.getFormattedDate());
      map.put(JsonKey.STATUS, ProjectUtil.BulkProcessStatus.IN_PROGRESS.getValue());
      cassandraOperation.updateRecord(bulkDb.getKeySpace(), bulkDb.getTableName(), map);
    } catch (Exception ex) {
      ProjectLogger.log(
          "Exception occurred while updating status to bulk_upload_process "
              + "table in BulkUploadBackGroundJobActor.",
          ex);
    }
    Response res =
        cassandraOperation.getRecordById(bulkDb.getKeySpace(), bulkDb.getTableName(), processId);
    return (((List<Map<String, Object>>) res.get(JsonKey.RESPONSE)).get(0));
  }

  private void updateRecordToUserOrgTable(Map<String, Object> map, String updatedBy) {
    Util.DbInfo usrOrgDb = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.ID, map.get(JsonKey.ID));
    reqMap.put(JsonKey.IS_DELETED, false);
    reqMap.put(JsonKey.UPDATED_BY, updatedBy);
    reqMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
    try {
      cassandraOperation.updateRecord(usrOrgDb.getKeySpace(), usrOrgDb.getTableName(), reqMap);
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
    }
  }

  private Map<String, Object> findUser(Map<String, Object> requestedUserMap) {
    Map<String, Object> foundUserMap = null;
    String extId = (String) requestedUserMap.get(JsonKey.EXTERNAL_ID);
    String provider = (String) requestedUserMap.get(JsonKey.EXTERNAL_ID_PROVIDER);
    String idType = (String) requestedUserMap.get(JsonKey.EXTERNAL_ID_TYPE);
    String userName = (String) requestedUserMap.get(JsonKey.USERNAME);
    if (StringUtils.isNotBlank(extId)
        && StringUtils.isNotBlank(provider)
        && StringUtils.isNotBlank(idType)
        && StringUtils.isBlank(userName)) {
      foundUserMap = Util.getUserFromExternalId(requestedUserMap);
      if (MapUtils.isEmpty(foundUserMap)) {
        throw new ProjectCommonException(
            ResponseCode.externalIdNotFound.getErrorCode(),
            ProjectUtil.formatMessage(
                ResponseCode.externalIdNotFound.getErrorMessage(), extId, idType, provider),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }
    } else {
      foundUserMap = getRecordByLoginId(requestedUserMap);
    }
    return foundUserMap;
  }

  private Map<String, Object> insertRecordToKeyCloak(
      Map<String, Object> requestedUserMap, Map<String, Object> foundUserMap, String updatedBy)
      throws Exception {
    requestedUserMap.put(JsonKey.UPDATED_BY, updatedBy);
    if (MapUtils.isNotEmpty(foundUserMap)) {
      updateUser(requestedUserMap, foundUserMap);
    } else {
      createUser(requestedUserMap);
    }
    if (!StringUtils.isBlank((String) requestedUserMap.get(JsonKey.PASSWORD))) {
      requestedUserMap.put(
          JsonKey.PASSWORD,
          OneWayHashing.encryptVal((String) requestedUserMap.get(JsonKey.PASSWORD)));
    }
    return requestedUserMap;
  }

  private Map<String, Object> getRecordByLoginId(Map<String, Object> userMap) {
    Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
    Map<String, Object> user = null;
    userMap.put(JsonKey.LOGIN_ID, Util.getLoginId(userMap));
    String loginId = Util.getEncryptedData((String) userMap.get(JsonKey.LOGIN_ID));
    Response resultFrUserName =
        cassandraOperation.getRecordsByProperty(
            usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), JsonKey.LOGIN_ID, loginId);
    if (CollectionUtils.isNotEmpty(
        (List<Map<String, Object>>) resultFrUserName.get(JsonKey.RESPONSE))) {
      user = ((List<Map<String, Object>>) resultFrUserName.get(JsonKey.RESPONSE)).get(0);
    }
    return user;
  }

  private void createUser(Map<String, Object> userMap) throws Exception {
    // user doesn't exist
    validateExternalIds(userMap, JsonKey.CREATE);

    try {
      String userId = "";
      userMap.put(JsonKey.BULK_USER_UPLOAD, true);
      Util.checkEmailUniqueness(userMap, JsonKey.CREATE);
      Util.checkPhoneUniqueness(userMap, JsonKey.CREATE);
      Map<String, String> userKeyClaokResp = ssoManager.createUser(userMap);
      userMap.remove(JsonKey.BULK_USER_UPLOAD);
      userId = userKeyClaokResp.get(JsonKey.USER_ID);
      if (!StringUtils.isBlank(userId)) {
        userMap.put(JsonKey.USER_ID, userId);
        userMap.put(JsonKey.ID, userId);
      } else {
        throw new ProjectCommonException(
            ResponseCode.userRegUnSuccessfull.getErrorCode(),
            ResponseCode.userRegUnSuccessfull.getErrorMessage(),
            ResponseCode.SERVER_ERROR.getResponseCode());
      }
    } catch (Exception exception) {
      ProjectLogger.log("Exception occurred while creating user in keycloak ", exception);
      throw exception;
    }
    userMap.put(JsonKey.CREATED_DATE, ProjectUtil.getFormattedDate());
    userMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
    userMap.put(JsonKey.IS_DELETED, false);
    if (!StringUtils.isBlank((String) userMap.get(JsonKey.COUNTRY_CODE))) {
      userMap.put(
          JsonKey.COUNTRY_CODE, propertiesCache.getProperty("sunbird_default_country_code"));
    }
    /**
     * set role as PUBLIC by default if role is empty in request body. And if roles are coming in
     * request body, then check for PUBLIC role , if not present then add PUBLIC role to the list
     */
    if (null == userMap.get(JsonKey.ROLES)) {
      userMap.put(JsonKey.ROLES, new ArrayList<String>());
    }
    List<String> roles = (List<String>) userMap.get(JsonKey.ROLES);
    if (!roles.contains(ProjectUtil.UserRole.PUBLIC.getValue())) {
      roles.add(ProjectUtil.UserRole.PUBLIC.getValue());
      userMap.put(JsonKey.ROLES, roles);
    }
  }

  private void updateUser(Map<String, Object> userMap, Map<String, Object> userDbRecord) {
    // user exist
    if (null != userDbRecord.get(JsonKey.IS_DELETED)
        && (boolean) userDbRecord.get(JsonKey.IS_DELETED)) {
      throw new ProjectCommonException(
          ResponseCode.inactiveUser.getErrorCode(),
          ResponseCode.inactiveUser.getErrorMessage(),
          ResponseCode.CLIENT_ERROR.getResponseCode());
    }

    userMap.put(JsonKey.ID, userDbRecord.get(JsonKey.ID));
    userMap.put(JsonKey.USER_ID, userDbRecord.get(JsonKey.ID));
    userMap.put(JsonKey.OPERATION, JsonKey.UPDATE);

    validateExternalIds(userMap, JsonKey.UPDATE);

    Util.checkEmailUniqueness(userMap, JsonKey.UPDATE);
    Util.checkPhoneUniqueness(userMap, JsonKey.UPDATE);
    String email = "";
    try {
      email = encryptionService.encryptData((String) userMap.get(JsonKey.EMAIL));
    } catch (Exception ex) {
      ProjectLogger.log(
          "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor during encryption of loginId:",
          ex);
      throw new ProjectCommonException(
          ResponseCode.userDataEncryptionError.getErrorCode(),
          ResponseCode.userDataEncryptionError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
    if (null != (String) userDbRecord.get(JsonKey.EMAIL)
        && ((String) userDbRecord.get(JsonKey.EMAIL)).equalsIgnoreCase(email)) {
      // DB email value and req email value both are same , no need to update
      email = (String) userMap.get(JsonKey.EMAIL);
      userMap.remove(JsonKey.EMAIL);
    }
    // check user is active for this organization or not
    isUserDeletedFromOrg(userMap, (String) userMap.get(JsonKey.UPDATED_BY));
    updateKeyCloakUserBase(userMap);
    email = decryptionService.decryptData(email);
    userMap.put(JsonKey.EMAIL, email);
  }

  private void validateExternalIds(Map<String, Object> userMap, String operation) {
    if (CollectionUtils.isNotEmpty((List<Map<String, String>>) userMap.get(JsonKey.EXTERNAL_IDS))) {
      List<Map<String, String>> list =
          Util.copyAndConvertExternalIdsToLower(
              (List<Map<String, String>>) userMap.get(JsonKey.EXTERNAL_IDS));
      userMap.put(JsonKey.EXTERNAL_IDS, list);
    }
    User user = mapper.convertValue(userMap, User.class);
    Util.checkExternalIdUniqueness(user, operation);
    if (JsonKey.UPDATE.equalsIgnoreCase(operation)) {
      Util.validateUserExternalIds(userMap);
    }
  }

  private boolean isUserDeletedFromOrg(Map<String, Object> userMap, String updatedBy) {
    Util.DbInfo usrOrgDbInfo = Util.dbInfoMap.get(JsonKey.USER_ORG_DB);
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.USER_ID, userMap.get(JsonKey.ID));
    map.put(JsonKey.ORGANISATION_ID, userMap.get(JsonKey.ORGANISATION_ID));
    Response response =
        cassandraOperation.getRecordsByProperties(
            usrOrgDbInfo.getKeySpace(), usrOrgDbInfo.getTableName(), map);
    List<Map<String, Object>> resList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
    if (!resList.isEmpty()) {
      Map<String, Object> res = resList.get(0);
      if (null != res.get(JsonKey.IS_DELETED)) {
        boolean bool = (boolean) (res.get(JsonKey.IS_DELETED));
        // if deleted then add this user to org and proceed
        try {
          if (bool) {
            updateRecordToUserOrgTable(res, updatedBy);
          }
        } catch (Exception ex) {
          throw new ProjectCommonException(
              ResponseCode.userUpdateToOrgFailed.getErrorCode(),
              ResponseCode.userUpdateToOrgFailed.getErrorMessage(),
              ResponseCode.CLIENT_ERROR.getResponseCode());
        }
        return bool;
      } else {
        return false;
      }
    }
    return false;
  }

  private String validateUser(Map<String, Object> userMap) {
    if (null != userMap.get(JsonKey.EXTERNAL_IDS)) {
      try {
        parseExternalIds(userMap);
      } catch (Exception ex) {
        return ProjectUtil.formatMessage(
            ResponseMessage.Message.PARSING_FAILED, JsonKey.EXTERNAL_IDS);
      }
    }
    userMap.put(JsonKey.EMAIL_VERIFIED, false);
    if (!StringUtils.isBlank((String) userMap.get(JsonKey.PHONE_VERIFIED))) {
      try {
        userMap.put(
            JsonKey.PHONE_VERIFIED,
            Boolean.parseBoolean((String) userMap.get(JsonKey.PHONE_VERIFIED)));
      } catch (Exception ex) {
        return ProjectUtil.formatMessage(
            ResponseMessage.Message.DATA_TYPE_ERROR, JsonKey.PHONE_VERIFIED, "Boolean");
      }
    }
    if (null != userMap.get(JsonKey.ROLES)) {
      convertCommaSepStringToList(userMap, JsonKey.ROLES);
    }
    if (null != userMap.get(JsonKey.GRADE)) {
      convertCommaSepStringToList(userMap, JsonKey.GRADE);
    }

    if (null != userMap.get(JsonKey.SUBJECT)) {
      convertCommaSepStringToList(userMap, JsonKey.SUBJECT);
    }

    if (null != userMap.get(JsonKey.LANGUAGE)) {
      convertCommaSepStringToList(userMap, JsonKey.LANGUAGE);
    }
    Request request = new Request();
    request.getRequest().putAll(userMap);
    try {
      UserRequestValidator.validateBulkUserData(request);
    } catch (ProjectCommonException ex) {
      return ex.getMessage();
    }

    return JsonKey.SUCCESS;
  }

  private String generatePrimaryKey(Map<String, Object> req) {
    String userId = (String) req.get(JsonKey.USER_ID);
    String courseId = (String) req.get(JsonKey.COURSE_ID);
    String batchId = (String) req.get(JsonKey.BATCH_ID);
    return OneWayHashing.encryptVal(
        userId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + courseId
            + JsonKey.PRIMARY_KEY_DELIMETER
            + batchId);
  }

  /**
   * This method will make some requested key value as lower case.
   *
   * @param map Request
   */
  public static void updateMapSomeValueTOLowerCase(Map<String, Object> map) {
    if (map.get(JsonKey.SOURCE) != null) {
      map.put(JsonKey.SOURCE, ((String) map.get(JsonKey.SOURCE)).toLowerCase());
    }
    if (map.get(JsonKey.EXTERNAL_ID) != null) {
      map.put(JsonKey.EXTERNAL_ID, ((String) map.get(JsonKey.EXTERNAL_ID)).toLowerCase());
    }
    if (map.get(JsonKey.USERNAME) != null) {
      map.put(JsonKey.USERNAME, ((String) map.get(JsonKey.USERNAME)).toLowerCase());
    }
    if (map.get(JsonKey.USER_NAME) != null) {
      map.put(JsonKey.USER_NAME, ((String) map.get(JsonKey.USER_NAME)).toLowerCase());
    }
    if (map.get(JsonKey.PROVIDER) != null) {
      map.put(JsonKey.PROVIDER, ((String) map.get(JsonKey.PROVIDER)).toLowerCase());
    }
    if (map.get(JsonKey.LOGIN_ID) != null) {
      map.put(JsonKey.LOGIN_ID, ((String) map.get(JsonKey.LOGIN_ID)).toLowerCase());
    }
  }

  private void updateKeyCloakUserBase(Map<String, Object> userMap) {
    try {
      String userId = ssoManager.updateUser(userMap);
      if (!(!StringUtils.isBlank(userId) && userId.equalsIgnoreCase(JsonKey.SUCCESS))) {
        throw new ProjectCommonException(
            ResponseCode.userUpdationUnSuccessfull.getErrorCode(),
            ResponseCode.userUpdationUnSuccessfull.getErrorMessage(),
            ResponseCode.SERVER_ERROR.getResponseCode());
      }
    } catch (Exception e) {
      ProjectLogger.log(e.getMessage(), e);
      throw new ProjectCommonException(
          ResponseCode.userUpdationUnSuccessfull.getErrorCode(),
          ResponseCode.userUpdationUnSuccessfull.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
  }

  // method will compare two strings and return true id both are same otherwise
  // false ...
  private boolean compareStrings(String first, String second) {

    if (isNull(first) && isNull(second)) {
      return true;
    }
    if ((isNull(first) && isNotNull(second)) || (isNull(second) && isNotNull(first))) {
      return false;
    }
    return first.equalsIgnoreCase(second);
  }

  private void saveAuditLog(Response result, String operation, Request message) {
    AuditOperation auditOperation = (AuditOperation) Util.auditLogUrlMap.get(operation);
    if (auditOperation.getObjectType().equalsIgnoreCase(JsonKey.USER)) {
      try {
        Map<String, Object> map = new HashMap<>();
        map.putAll((Map<String, Object>) message.getRequest().get(JsonKey.USER));
        UserUtility.encryptUserData(map);
        message.getRequest().put(JsonKey.USER, map);
      } catch (Exception ex) {
        ProjectLogger.log(
            "Exception occurred while bulk user upload in BulkUploadBackGroundJobActor during data encryption :",
            ex);
      }
    }
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.OPERATION, auditOperation);
    map.put(JsonKey.REQUEST, message);
    map.put(JsonKey.RESPONSE, result);
    Request request = new Request();
    request.setOperation(ActorOperations.PROCESS_AUDIT_LOG.getValue());
    request.setRequest(map);
    tellToAnother(request);
  }

  /**
   * Fields which are not allowed to update while updating user info.
   *
   * @param userMap
   */
  private void removeFieldsFrmUpdateReq(Map<String, Object> userMap) {
    userMap.remove(JsonKey.OPERATION);
    userMap.remove(JsonKey.EXTERNAL_IDS);
    userMap.remove(JsonKey.ENC_EMAIL);
    userMap.remove(JsonKey.ENC_PHONE);
    userMap.remove(JsonKey.EMAIL_VERIFIED);
    userMap.remove(JsonKey.STATUS);
    userMap.remove(JsonKey.USERNAME);
    userMap.remove(JsonKey.ROOT_ORG_ID);
    userMap.remove(JsonKey.LOGIN_ID);
    userMap.remove(JsonKey.ROLES);
    userMap.remove(JsonKey.CHANNEL);
  }

  private boolean isSlugUnique(String slug) {
    if (!StringUtils.isBlank(slug)) {
      Map<String, Object> filters = new HashMap<>();
      filters.put(JsonKey.SLUG, slug);
      filters.put(JsonKey.IS_ROOT_ORG, true);
      Map<String, Object> esResult =
          elasticSearchComplexSearch(
              filters, EsIndex.sunbird.getIndexName(), EsType.organisation.getTypeName());
      if (isNotNull(esResult)
          && esResult.containsKey(JsonKey.CONTENT)
          && isNotNull(esResult.get(JsonKey.CONTENT))) {
        return (((List) esResult.get(JsonKey.CONTENT)).isEmpty());
      }
    }
    return false;
  }
}
