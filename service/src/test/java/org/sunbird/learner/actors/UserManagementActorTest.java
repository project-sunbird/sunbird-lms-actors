package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.actor.router.RequestRouter;
import org.sunbird.actorutil.InterServiceCommunication;
import org.sunbird.actorutil.InterServiceCommunicationFactory;
import org.sunbird.actorutil.impl.InterServiceCommunicationImpl;
import org.sunbird.actorutil.systemsettings.impl.SystemSettingClientImpl;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.user.actors.UserManagementActor;
import org.sunbird.user.service.UserService;
import org.sunbird.user.service.impl.UserServiceImpl;
import org.sunbird.user.util.UserUtil;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  ElasticSearchUtil.class,
  Util.class,
  RequestRouter.class,
  SystemSettingClientImpl.class,
  UserServiceImpl.class,
  UserUtil.class,
  InterServiceCommunicationFactory.class,
})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*", "javax.net.ssl.*", "javax.security.*"})
public class UserManagementActorTest {

  public ActorSystem system = ActorSystem.create("system");
  public static final Props props = Props.create(UserManagementActor.class);
  public static UserService userService;

  private static Map<String, Object> reqMap;

  @Before
  public void beforeEachTest() {

    ActorRef actorRef = mock(ActorRef.class);
    PowerMockito.mockStatic(RequestRouter.class);
    when(RequestRouter.getActor(Mockito.anyString())).thenReturn(actorRef);

    PowerMockito.mockStatic(ServiceFactory.class);
    CassandraOperationImpl cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
    when(cassandraOperation.insertRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());
    when(cassandraOperation.updateRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(getSuccessResponse());

    PowerMockito.mockStatic(InterServiceCommunicationFactory.class);
    InterServiceCommunication interServiceCommunication = mock(InterServiceCommunicationImpl.class);
    when(InterServiceCommunicationFactory.getInstance()).thenReturn(interServiceCommunication);
    when(interServiceCommunication.getResponse(
            Mockito.any(ActorRef.class), Mockito.any(Request.class)))
        .thenReturn(getEsResponse());

    PowerMockito.mockStatic(SystemSettingClientImpl.class);
    SystemSettingClientImpl systemSettingClient = mock(SystemSettingClientImpl.class);
    when(SystemSettingClientImpl.getInstance()).thenReturn(systemSettingClient);

    when(systemSettingClient.getSystemSettingByFieldAndKey(
            Mockito.any(ActorRef.class),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyObject()))
        .thenReturn(new HashMap<>());

    PowerMockito.mockStatic(UserServiceImpl.class);
    userService = mock(UserServiceImpl.class);
    when(UserServiceImpl.getInstance()).thenReturn(userService);
    when(userService.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn("anyId");
    when(userService.getCustodianChannel(Mockito.anyMap(), Mockito.any(ActorRef.class)))
        .thenReturn("anyChannel");
    when(userService.getRootOrgIdFromChannel(Mockito.anyString())).thenReturn("rootOrgId");

    PowerMockito.mockStatic(ElasticSearchUtil.class);
    when(ElasticSearchUtil.getDataByIdentifier(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getEsResponseMap());

    PowerMockito.mockStatic(Util.class);
    Util.getUserProfileConfig(Mockito.any(ActorRef.class));

    PowerMockito.mockStatic(UserUtil.class);
    UserUtil.setUserDefaultValue(Mockito.anyMap(), Mockito.anyString());

    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put(JsonKey.TNC_ACCEPTED_ON, 12345678L);
    when(UserUtil.encryptUserData(Mockito.anyMap())).thenReturn(requestMap);

    reqMap = getMapObject();
  }

  @Test
  public void testCreateUserSuccessWithUserCallerId() {

    boolean result =
        testScenario(
            getRequestedObj(
                true, true, true, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutUserCallerId() {

    boolean result =
        testScenario(
            getRequestedObj(
                false, true, true, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutUserCallerIdChannelAndRootOrgId() {

    boolean result =
        testScenario(
            getRequestedObj(false, false, true, reqMap, ActorOperations.CREATE_USER), null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidChannelAndOrgId() {

    reqMap.put(JsonKey.CHANNEL, "anyReqChannel");
    reqMap.put(JsonKey.ORGANISATION_ID, "anyOrgId");
    boolean result =
        testScenario(
            getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.parameterMismatch);
    assertTrue(result);
  }

  @Test
  public void testCreateUserSuccessWithoutVersion() {

    boolean result =
        testScenario(
            getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER), null);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidExternalIds() {

    reqMap.put(JsonKey.EXTERNAL_IDS, "anyExternalId");
    boolean result =
        testScenario(
            getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.dataTypeError);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidRoles() {

    reqMap.put(JsonKey.ROLES, "anyRoles");
    boolean result =
        testScenario(
            getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.dataTypeError);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidCountryCode() {

    reqMap.put(JsonKey.COUNTRY_CODE, "anyCode");
    boolean result =
        testScenario(
            getRequestedObj(false, false, false, reqMap, ActorOperations.CREATE_USER),
            ResponseCode.invalidCountryCode);
    assertTrue(result);
  }

  @Test
  public void testCreateUserFailureWithInvalidOrg() {

    when(ElasticSearchUtil.getDataByIdentifier(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(null);
    boolean result =
        testScenario(
            getRequestedObj(
                false, false, false, getAdditionalMapData(reqMap), ActorOperations.CREATE_USER),
            ResponseCode.invalidOrgData);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserSuccess() {

    boolean result =
        testScenario(
            getRequestedObj(true, true, true, getExternalIdMap(), ActorOperations.UPDATE_USER),
            null);
    assertTrue(result);
  }

  @Test
  public void testUpdateUserSuccessWithoutUserCallerId() {

    boolean result =
        testScenario(
            getRequestedObj(false, true, true, getExternalIdMap(), ActorOperations.UPDATE_USER),
            null);
    assertTrue(result);
  }

  private Map<String, Object> getAdditionalMapData(Map<String, Object> reqMap) {
    reqMap.put(JsonKey.ORGANISATION_ID, "anyOrgId");
    reqMap.put(JsonKey.CHANNEL, "anyChannel");
    return reqMap;
  }

  private boolean testScenario(Request reqObj, ResponseCode errorCode) {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    subject.tell(reqObj, probe.getRef());

    if (errorCode == null) {
      Response res = probe.expectMsgClass(duration("10 second"), Response.class);
      return null != res && res.getResponseCode() == ResponseCode.OK;
    } else {
      ProjectCommonException res =
          probe.expectMsgClass(duration("10 second"), ProjectCommonException.class);
      return res.getCode().equals(errorCode.getErrorCode())
          || res.getResponseCode() == errorCode.getResponseCode();
    }
  }

  private Map<String, Object> getExternalIdMap() {

    Map<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.EXTERNAL_ID, "externalId");
    reqMap.put(JsonKey.EXTERNAL_ID_PROVIDER, "externalIdProvider");
    reqMap.put(JsonKey.EXTERNAL_ID_TYPE, "externalIdType");
    return reqMap;
  }

  private HashMap<String, Object> getMapObject() {

    HashMap<String, Object> reqMap = new HashMap<>();
    reqMap.put(JsonKey.FIRST_NAME, "firstname");
    reqMap.put(JsonKey.USERNAME, "userName");
    reqMap.put(JsonKey.EMAIL, "email@email.com");
    reqMap.put(JsonKey.LANGUAGE, new ArrayList<>());
    reqMap.put(JsonKey.DOB, "1992-12-31");
    reqMap.put(JsonKey.EMAIL_VERIFIED, true);
    reqMap.put(JsonKey.PHONE_VERIFIED, true);
    reqMap.put(JsonKey.ADDRESS, new ArrayList<>());
    return reqMap;
  }

  private Request getRequestedObj(
      boolean isCallerIdReq,
      boolean isRootOrgIdReq,
      boolean isVersionReq,
      Map<String, Object> reqMap,
      ActorOperations actorOperation) {

    Request reqObj = new Request();
    HashMap<String, Object> innerMap = new HashMap<>();
    if (isCallerIdReq) innerMap.put(JsonKey.CALLER_ID, "anyCallerId");
    if (isVersionReq) innerMap.put(JsonKey.VERSION, "v2");
    if (isRootOrgIdReq) innerMap.put(JsonKey.ROOT_ORG_ID, "MY_ROOT_ORG_ID");
    innerMap.put(JsonKey.REQUESTED_BY, "requestedBy");
    reqObj.setRequest(reqMap);
    reqObj.setContext(innerMap);
    reqObj.setOperation(actorOperation.getValue());
    return reqObj;
  }

  private static Response getEsResponse() {
    Response response = new Response();
    Map<String, Object> map = new HashMap<>();
    map.put("anyString", new Object());
    map.put(JsonKey.ERRORS, null);
    response.put(JsonKey.RESPONSE, map);
    return response;
  }

  private static Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
    return response;
  }

  private static Map<String, Object> getEsResponseMap() {
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.IS_ROOT_ORG, true);
    map.put(JsonKey.ID, "rootOrgId");
    map.put(JsonKey.CHANNEL, "anyChannel");
    return map;
  }
}
