package org.sunbird.user.actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.bean.ClaimStatus;
import org.sunbird.bean.ShadowUser;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchHelper;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.factory.EsClientFactory;
import org.sunbird.common.inf.ElasticSearchService;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.UserFlagEnum;
import org.sunbird.learner.util.UserFlagUtil;
import org.sunbird.learner.util.Util;
import org.sunbird.models.user.UserType;
import org.sunbird.telemetry.util.TelemetryUtil;
import org.sunbird.user.util.MigrationUtils;
import scala.concurrent.Future;

import java.sql.Timestamp;
import java.util.*;


@ActorConfig(
        tasks = {"migrateUser"},
        asyncTasks = {}
)
public class MigrationActor extends BaseActor {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private String custodianOrgId;
    private Map<String, Map<String, Object>> processIdtelemetryCtxMap = new HashMap<>();
    private ElasticSearchService elasticSearchService = EsClientFactory.getInstance(JsonKey.REST);
    private ObjectMapper mapper = new ObjectMapper();
    private Map<String, String> hashTagIdMap = new HashMap<>();
    private Util.DbInfo bulkUploadDbInfo = Util.dbInfoMap.get(JsonKey.BULK_OP_DB);
    private Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);


    @Override
    public void onReceive(Request request) throws Throwable {
        migrate(request);
    }


    private void migrate(Request request) {
        ShadowUser shadowUser = MigrationUtils.getRecord(request.getRequest());
        if (null == shadowUser) {
            // TODO INCREASE ATTEMPT COUNT
            ProjectCommonException.throwClientErrorException(ResponseCode.invalidUsrData);
        }
            if (isUserAgreed(request)) {
                if(shadowUser.getClaimStatus()==ClaimStatus.ELIGIBLE.getValue()) {
                    performMigration(shadowUser);
                }
            } else {
                MigrationUtils.markUserAsRejected(shadowUser);
            }
    }


    private boolean isUserAgreed(Request request) {
        if ((int) request.getRequest().get(JsonKey.ACTION) == ProjectUtil.Action.YES.getValue()) {
            return true;
        }
        return false;
    }

    private void performMigration(ShadowUser shadowUser) {
        updateUser(shadowUser);
    }


    private void updateUser(ShadowUser shadowUser) {
        List<Map<String, Object>> esUser = getUserMatchedIdentifierFromES(shadowUser);
        ProjectLogger.log("MigrationActor:updateUser:GOT ES RESPONSE FOR USER WITH SIZE " + esUser.size(), LoggerEnum.INFO.name());
        if (CollectionUtils.isNotEmpty(esUser)) {
            ProjectLogger.log("MigrationActor:updateUser:Got single user:" + esUser + " :with processId" + shadowUser.getProcessId(), LoggerEnum.INFO.name());
            Map<String, Object> userMap = esUser.get(0);
            ProjectLogger.log("MigrationActor:updateUser: provided user details doesn't match with existing user details with processId" + shadowUser.getProcessId() + userMap, LoggerEnum.INFO.name());
            String rootOrgId = getRootOrgIdFromChannel(shadowUser.getChannel());
            int flagsValue = null != userMap.get(JsonKey.FLAGS_VALUE) ? (int) userMap.get(JsonKey.FLAGS_VALUE) : 0;   // since we are migrating the user from custodian org to non custodian org.
            ProjectLogger.log("MigrationActor:processClaimedUser:Got Flag Value " + flagsValue, LoggerEnum.INFO.name());
            updateUserInUserTable(flagsValue, (String) userMap.get(JsonKey.ID), rootOrgId, shadowUser);
            String orgIdFromOrgExtId = getOrgId(shadowUser);
            updateUserOrg(orgIdFromOrgExtId, rootOrgId, userMap);
            createUserExternalId((String) userMap.get(JsonKey.ID), shadowUser);
            updateUserInShadowDb((String) userMap.get(JsonKey.ID), shadowUser, ClaimStatus.CLAIMED.getValue(), null);
            syncUserToES((String) userMap.get(JsonKey.ID));
        } else {
            ProjectLogger.log("MigrationActor:updateUser:SKIPPING SHADOW USER:" + shadowUser.toString(), LoggerEnum.INFO.name());
        }
        esUser.clear();
    }

    private void updateUserOrg(String orgIdFromOrgExtId, String rootOrgId, Map<String, Object> userMap) {
        deleteUserOrganisations(userMap);
        ProjectLogger.log("MigrationActor:updateUserOrg:deleting user organisation completed no started registering user to org", LoggerEnum.INFO.name());
        registerUserToOrg((String) userMap.get(JsonKey.ID), rootOrgId);
        if (StringUtils.isNotBlank(orgIdFromOrgExtId) && !StringUtils.equalsIgnoreCase(rootOrgId, orgIdFromOrgExtId)) {
            ProjectLogger.log("MigrationActor:updateUserOrg:user also needs to register with sub org", LoggerEnum.INFO.name());
            registerUserToOrg((String) userMap.get(JsonKey.ID), orgIdFromOrgExtId);
        }
    }

    private String getOrgId(ShadowUser shadowUser) {
        if (StringUtils.isNotBlank(shadowUser.getOrgExtId())) {
            Map<String, Object> request = new HashMap<>();
            Map<String, Object> filters = new HashMap<>();
            filters.put(JsonKey.EXTERNAL_ID, shadowUser.getOrgExtId().toLowerCase());
            filters.put(JsonKey.CHANNEL, shadowUser.getChannel());
            request.put(JsonKey.FILTERS, filters);
            ProjectLogger.log("ShadowUserProcessor:getOrgId: request map prepared to query elasticsearch for org id :" + filters + "with processId" + shadowUser.getProcessId(), LoggerEnum.INFO.name());
            SearchDTO searchDTO = ElasticSearchHelper.createSearchDTO(request);
            Map<String, Object> response = (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(elasticSearchService.search(searchDTO, ProjectUtil.EsType.organisation.getTypeName()));
            List<Map<String, Object>> orgData = ((List<Map<String, Object>>) response.get(JsonKey.CONTENT));
            if (CollectionUtils.isNotEmpty(orgData)) {
                Map<String, Object> orgMap = orgData.get(0);
                return (String) orgMap.get(JsonKey.ID);
            }
        }
        return StringUtils.EMPTY;
    }


    private void updateUserInShadowDb(String userId, ShadowUser shadowUser, int claimStatus, List<String> matchingUserIds) {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(JsonKey.CLAIM_STATUS, claimStatus);
        propertiesMap.put(JsonKey.PROCESS_ID, shadowUser.getProcessId());
        if (claimStatus == ClaimStatus.ELIGIBLE.getValue()) {
            propertiesMap.put(JsonKey.CLAIMED_ON, new Timestamp(System.currentTimeMillis()));
            propertiesMap.put(JsonKey.USER_ID, userId);
        }
        if (null != matchingUserIds && CollectionUtils.isNotEmpty(matchingUserIds)) {
            propertiesMap.put(JsonKey.USER_IDs, matchingUserIds);
        }
        Map<String, Object> compositeKeysMap = new HashMap<>();
        compositeKeysMap.put(JsonKey.CHANNEL, shadowUser.getChannel());
        compositeKeysMap.put(JsonKey.USER_EXT_ID, shadowUser.getUserExtId());
        Response response = cassandraOperation.updateRecord(JsonKey.SUNBIRD, JsonKey.SHADOW_USER, propertiesMap, compositeKeysMap);
        ProjectLogger.log("ShadowUserProcessor:updateUserInShadowDb:update:with processId: " + shadowUser.getProcessId() + " :and response is:" + response, LoggerEnum.INFO.name());
    }

    private void updateStatusInUserOrg(ShadowUser shadowUser, String id) {
        Map<String, Object> propertiesMap = new WeakHashMap<>();
        propertiesMap.put(JsonKey.ID, id);
        propertiesMap.put(JsonKey.IS_DELETED, true);
        propertiesMap.put(JsonKey.UPDATED_BY, shadowUser.getAddedBy());
        propertiesMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        Response response = cassandraOperation.updateRecord(JsonKey.SUNBIRD, JsonKey.USER_ORG, propertiesMap);
        ProjectLogger.log("ShadowUserProcessor:updateStatusInUserOrg:response from cassandra in updating user org ".concat(response + ""), LoggerEnum.INFO.name());
    }

    private List<Map<String, Object>> getUserMatchedIdentifierFromES(ShadowUser shadowUser) {
        Map<String, Object> request = new WeakHashMap<>();
        Map<String, Object> filters = new WeakHashMap<>();
        Map<String, Object> or = new WeakHashMap<>();
        if (StringUtils.isNotBlank(shadowUser.getEmail())) {
            or.put(JsonKey.EMAIL, shadowUser.getEmail());
        }
        if (StringUtils.isNotBlank(shadowUser.getPhone())) {
            or.put(JsonKey.PHONE, shadowUser.getPhone());
        }
        filters.put(JsonKey.ES_OR_OPERATION, or);
        filters.put(JsonKey.ROOT_ORG_ID, getCustodianOrgId());
        request.put(JsonKey.FILTERS, filters);
        ProjectLogger.log("ShadowUserProcessor:getUserMatchedIdentifierFromES:the filter prepared for elastic search with processId: " + shadowUser.getProcessId() + " :filters are:" + filters, LoggerEnum.INFO.name());
        SearchDTO searchDTO = ElasticSearchHelper.createSearchDTO(request);
        searchDTO.setFields(new ArrayList<String>(Arrays.asList(JsonKey.ID, JsonKey.CHANNEL, JsonKey.EMAIL, JsonKey.PHONE, JsonKey.ROOT_ORG_ID, JsonKey.FLAGS_VALUE, JsonKey.ORGANISATIONS, JsonKey.IS_DELETED, JsonKey.STATUS)));
        Map<String, Object> response = (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(elasticSearchService.search(searchDTO, JsonKey.USER));
        ProjectLogger.log("ShadowUserProcessor:getUserMatchedIdentifierFromES:response got from elasticSearch is with processId: " + shadowUser.getProcessId() + " :response is" + response, LoggerEnum.INFO.name());
        return (List<Map<String, Object>>) response.get(JsonKey.CONTENT);
    }

    private void updateUserInUserTable(int flagValue, String userId, String rootOrgId, ShadowUser shadowUser) {
        Map<String, Object> propertiesMap = new WeakHashMap<>();
        propertiesMap.put(JsonKey.FIRST_NAME, shadowUser.getName());
        propertiesMap.put(JsonKey.ID, userId);
        if (!(UserFlagUtil.assignUserFlagValues(flagValue).get(JsonKey.STATE_VALIDATED))) {
            ProjectLogger.log("ShadowUserProcessor:updateUserInUserTable: updating Flag Value", LoggerEnum.INFO.name());
            propertiesMap.put(JsonKey.FLAGS_VALUE, flagValue + UserFlagEnum.STATE_VALIDATED.getUserFlagValue());
        }
        propertiesMap.put(JsonKey.UPDATED_BY, shadowUser.getAddedBy());
        propertiesMap.put(JsonKey.UPDATED_DATE, ProjectUtil.getFormattedDate());
        if (shadowUser.getUserStatus() == ProjectUtil.Status.ACTIVE.getValue()) {
            propertiesMap.put(JsonKey.IS_DELETED, false);
            propertiesMap.put(JsonKey.STATUS, ProjectUtil.Status.ACTIVE.getValue());
        } else {
            propertiesMap.put(JsonKey.IS_DELETED, true);
            propertiesMap.put(JsonKey.STATUS, ProjectUtil.Status.INACTIVE.getValue());
        }
        propertiesMap.put(JsonKey.USER_TYPE, UserType.TEACHER.getTypeName());
        propertiesMap.put(JsonKey.CHANNEL, shadowUser.getChannel());
        propertiesMap.put(JsonKey.ROOT_ORG_ID, rootOrgId);
        ProjectLogger.log("ShadowUserProcessor:updateUserInUserTable: properties map formed for user update: " + propertiesMap, LoggerEnum.INFO.name());
        Response response = cassandraOperation.updateRecord(usrDbInfo.getKeySpace(), usrDbInfo.getTableName(), propertiesMap);
        ProjectLogger.log("ShadowUserProcessor:updateUserInUserTable:user is updated with shadow user:RESPONSE FROM CASSANDRA IS:" + response.getResult(), LoggerEnum.INFO.name());
        generateTelemetry(userId, rootOrgId, shadowUser);
    }


    private String getRootOrgIdFromChannel(String channel) {
        Map<String, Object> request = new WeakHashMap<>();
        Map<String, Object> filters = new WeakHashMap<>();
        filters.put(JsonKey.CHANNEL, channel);
        filters.put(JsonKey.IS_ROOT_ORG, true);
        request.put(JsonKey.FILTERS, filters);
        SearchDTO searchDTO = ElasticSearchHelper.createSearchDTO(request);
        searchDTO.getAdditionalProperties().put(JsonKey.FILTERS, filters);
        Future<Map<String, Object>> esResultF = elasticSearchService.search(searchDTO, ProjectUtil.EsType.organisation.getTypeName());
        Map<String, Object> esResult = (Map<String, Object>) ElasticSearchHelper.getResponseFromFuture(esResultF);
        if (MapUtils.isNotEmpty(esResult) && CollectionUtils.isNotEmpty((List) esResult.get(JsonKey.CONTENT))) {
            Map<String, Object> esContent = ((List<Map<String, Object>>) esResult.get(JsonKey.CONTENT)).get(0);
            return (String) esContent.get(JsonKey.ID);
        }
        return StringUtils.EMPTY;
    }


    /**
     * this method
     *
     * @return
     */
    private String getCustodianOrgId() {
        if (StringUtils.isNotBlank(custodianOrgId)) {
            ProjectLogger.log("ShadowUserProcessor:getCustodianOrgId:CUSTODIAN ORD ID FOUND in cache:" + custodianOrgId, LoggerEnum.INFO.name());
            return custodianOrgId;
        }
        Response response = cassandraOperation.getRecordById(JsonKey.SUNBIRD, JsonKey.SYSTEM_SETTINGS_DB, JsonKey.CUSTODIAN_ORG_ID);
        List<Map<String, Object>> result = new ArrayList<>();
        if (!((List) response.getResult().get(JsonKey.RESPONSE)).isEmpty()) {
            result = ((List) response.getResult().get(JsonKey.RESPONSE));
            Map<String, Object> resultMap = result.get(0);
            custodianOrgId = (String) resultMap.get(JsonKey.VALUE);
            ProjectLogger.log("ShadowUserProcessor:getCustodianOrgId:CUSTODIAN ORD ID FOUND in DB:" + custodianOrgId, LoggerEnum.INFO.name());

        }

        if (StringUtils.isBlank(custodianOrgId)) {
            ProjectLogger.log("ShadowUserProcessor:getCustodianOrgId:No CUSTODIAN ORD ID FOUND PLEASE HAVE THAT IN YOUR ENVIRONMENT", LoggerEnum.ERROR.name());
            System.exit(-1);
        }
        return custodianOrgId;
    }


    private void generateTelemetry(String userId, String rootOrgId, ShadowUser shadowUser) {
        ExecutionContext.getCurrent().setRequestContext(getTelemetryContextByProcessId((String) shadowUser.getProcessId()));
        ProjectLogger.log("ShadowUserProcessor:generateTelemetry:generate telemetry:" + shadowUser.toString(), LoggerEnum.INFO.name());
        Map<String, Object> targetObject = new HashMap<>();
        Map<String, String> rollUp = new HashMap<>();
        rollUp.put("l1", rootOrgId);
        List<Map<String, Object>> correlatedObject = new ArrayList<>();
        ExecutionContext.getCurrent().getRequestContext().put(JsonKey.ROLLUP, rollUp);
        TelemetryUtil.generateCorrelatedObject(shadowUser.getProcessId(), JsonKey.PROCESS_ID, null, correlatedObject);
        targetObject =
                TelemetryUtil.generateTargetObject(
                        userId, StringUtils.capitalize(JsonKey.USER), JsonKey.MIGRATION_USER_OBJECT, null);
        TelemetryUtil.telemetryProcessingCall(mapper.convertValue(shadowUser, Map.class), targetObject, correlatedObject);
    }

    private Map<String, Object> getTelemetryContextByProcessId(String processId) {

        if (MapUtils.isNotEmpty(processIdtelemetryCtxMap.get(processId))) {
            return processIdtelemetryCtxMap.get(processId);
        }
        Map<String, String> contextMap = new HashMap<>();
        Map<String, Object> telemetryContext = new HashMap<>();
        Response response = cassandraOperation.getRecordById(bulkUploadDbInfo.getKeySpace(), bulkUploadDbInfo.getTableName(), processId);
        List<Map<String, Object>> result = new ArrayList<>();
        if (!((List) response.getResult().get(JsonKey.RESPONSE)).isEmpty()) {
            result = ((List) response.getResult().get(JsonKey.RESPONSE));
            Map<String, Object> responseMap = result.get(0);
            contextMap = (Map<String, String>) responseMap.get(JsonKey.CONTEXT_TELEMETRY);
            telemetryContext.putAll(contextMap);
            processIdtelemetryCtxMap.put(processId, telemetryContext);
        }
        ProjectLogger.log("ShadowUserMigrationScheduler:getFullRecordFromProcessId:got single row data from bulk_upload_process with processId:" + processId, LoggerEnum.INFO.name());
        return telemetryContext;
    }

    private List<String> getOrganisationIds(Map<String, Object> dbUser) {
        List<String> organisationsIds = new ArrayList<>();
        ((List<Map<String, Object>>) dbUser.get(JsonKey.ORGANISATIONS)).stream().forEach(organisation -> {
            organisationsIds.add((String) organisation.get(JsonKey.ORGANISATION_ID));
        });
        return organisationsIds;
    }


    private void syncUserToES(String userId) {
        Map<String, Object> fullUserDetails = Util.getUserDetails(userId, null);
        try {
            Future<Boolean> future = elasticSearchService.update(JsonKey.USER, userId, fullUserDetails);
            if ((boolean) ElasticSearchHelper.getResponseFromFuture(future)) {
                ProjectLogger.log("ShadowUserMigrationScheduler:updateUserStatus: data successfully updated to elastic search with userId:".concat(userId + ""), LoggerEnum.INFO.name());
            }
        } catch (Exception e) {
            e.printStackTrace();
            ProjectLogger.log("ShadowUserMigrationScheduler:syncUserToES: data failed to updates in elastic search with userId:".concat(userId + ""), LoggerEnum.ERROR.name());
        }
    }


    private void deleteUserOrganisations(Map<String, Object> esUserMap) {
        ((List<Map<String, Object>>) esUserMap.get(JsonKey.ORGANISATIONS)).stream().forEach(organisation -> {
            String id = (String) organisation.get(JsonKey.ID);
            deleteOrgFromUserOrg(id);
        });
    }

    private void deleteOrgFromUserOrg(String id) {
        Response response = cassandraOperation.deleteRecord(JsonKey.SUNBIRD, JsonKey.USER_ORG, id);
        ProjectLogger.log("ShadowUserProcessor:deleteOrgFromUserOrg:user org is deleted ".concat(response.getResult() + ""), LoggerEnum.INFO.name());
    }


    private void registerUserToOrg(String userId, String organisationId) {
        Map<String, Object> reqMap = new WeakHashMap<>();
        List<String> roles = new ArrayList<>();
        roles.add(ProjectUtil.UserRole.PUBLIC.getValue());
        reqMap.put(JsonKey.ROLES, roles);
        String hashTagId = hashTagIdMap.get(organisationId);
        if (StringUtils.isBlank(hashTagId)) {
            hashTagId = Util.getHashTagIdFromOrgId(organisationId);
            hashTagIdMap.put(organisationId, hashTagId);
        }
        reqMap.put(JsonKey.HASHTAGID, hashTagId);
        reqMap.put(JsonKey.ID, ProjectUtil.getUniqueIdFromTimestamp(1));
        reqMap.put(JsonKey.USER_ID, userId);
        reqMap.put(JsonKey.ORGANISATION_ID, organisationId);
        reqMap.put(JsonKey.ORG_JOIN_DATE, ProjectUtil.getFormattedDate());
        reqMap.put(JsonKey.IS_DELETED, false);
        Util.DbInfo usrOrgDb = Util.dbInfoMap.get(JsonKey.USR_ORG_DB);
        try {
            Response response = cassandraOperation.insertRecord(usrOrgDb.getKeySpace(), usrOrgDb.getTableName(), reqMap);
            ProjectLogger.log("ShadowUserProcessor:registerUserToOrg:user status while registration with org is:" + response.getResult(), LoggerEnum.INFO.name());

        } catch (Exception e) {
            ProjectLogger.log("ShadowUserProcessor:registerUserToOrg:user is failed to register with org" + userId, LoggerEnum.ERROR.name());
        }
    }

    private void createUserExternalId(String userId, ShadowUser shadowUser) {
        Map<String, Object> externalId = new WeakHashMap<>();
        externalId.put(JsonKey.ID_TYPE, shadowUser.getChannel().toLowerCase());
        externalId.put(JsonKey.PROVIDER, shadowUser.getChannel().toLowerCase());
        externalId.put(JsonKey.EXTERNAL_ID, shadowUser.getUserExtId().toLowerCase());
        externalId.put(JsonKey.ORIGINAL_EXTERNAL_ID, externalId.get(JsonKey.EXTERNAL_ID));
        externalId.put(JsonKey.ORIGINAL_PROVIDER, externalId.get(JsonKey.PROVIDER));
        externalId.put(JsonKey.ORIGINAL_ID_TYPE, externalId.get(JsonKey.ID_TYPE));
        externalId.put(JsonKey.USER_ID, userId);
        externalId.put(JsonKey.CREATED_BY, shadowUser.getAddedBy());
        externalId.put(JsonKey.CREATED_ON, new Timestamp(System.currentTimeMillis()));
        ProjectLogger.log("ShadowUserProcessor:createUserExternalId:map prepared for user externalid is " + externalId + "with processId" + shadowUser.getProcessId(), LoggerEnum.INFO.name());
        saveUserExternalId(externalId);
    }


    /**
     * this method will save user data in usr_external_identity table
     *
     * @param externalId
     */
    private void saveUserExternalId(Map<String, Object> externalId) {
        Response response = cassandraOperation.insertRecord(JsonKey.SUNBIRD, JsonKey.USR_EXT_IDNT_TABLE, externalId);
        ProjectLogger.log("ShadowUserProcessor:createUserExternalId:response from cassandra ".concat(response.getResult() + ""), LoggerEnum.INFO.name());
    }
}
