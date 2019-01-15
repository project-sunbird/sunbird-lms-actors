package org.sunbird.learner.actors;

import static akka.testkit.JavaTestKit.duration;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.content.util.ContentStoreUtil;
import org.sunbird.learner.util.DataCacheHandler;
import org.sunbird.learner.util.Util;

@PrepareForTest({DataCacheHandler.class, ContentStoreUtil.class})
public class UserFrameworkUpdateTest extends BaseUserManagementActorTest {

  private ActorSystem system = ActorSystem.create("system");
  private static final Props props = getProps();

  @Before
  public void beforeTest() {
    PowerMockito.mockStatic(DataCacheHandler.class);
    PowerMockito.mockStatic(ContentStoreUtil.class);
  }

  @Test
  public void testUpdateUserFrameworkSuccess() {
    Request reqObj = getRequest(null, null);
    Response res = doUpdateActorCallSuccess(reqObj);
    assertTrue(null != res.get(JsonKey.RESPONSE));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidGradeLevel() {
    Request reqObj = getRequest("gradeLevel", "SomeWrongGrade");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidMedium() {
    Request reqObj = getRequest("medium", "glish");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidBoard() {
    Request reqObj = getRequest("board", "RBCS");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.invalidParameterValue.getErrorCode()));
  }

  @Test
  public void testUpdateUserFrameworkFailureInvalidFrameworkId() {
    Request reqObj = getRequest(JsonKey.ID, "invalidFrameworkId");
    ProjectCommonException res = doUpdateActorCallFailure(reqObj);
    assertTrue(res.getCode().equals(ResponseCode.errorNoFrameworkFound.getErrorCode()));
  }

  private Response doUpdateActorCallSuccess(Request reqObj) {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockForUpdateTest();
    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("200 second"), Response.class);
    return res;
  }

  private ProjectCommonException doUpdateActorCallFailure(Request reqObj) {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);
    mockForUpdateTest();
    subject.tell(reqObj, probe.getRef());
    ProjectCommonException res =
        probe.expectMsgClass(duration("200 second"), ProjectCommonException.class);
    return res;
  }

  @SuppressWarnings("unchecked")
  public void mockForUpdateTest() {
    mockUtilsForOrgDetails();
    mockDatacacheHandler();
    mockContentStoreUtil();
  }

  private void mockUtilsForOrgDetails() {
    Map<String, Object> rootOrgMap = new HashMap<>();
    String hashTagId = "someHashTagId";
    rootOrgMap.put(JsonKey.HASHTAGID, hashTagId);
    when(Util.getOrgDetails(Mockito.anyString())).thenReturn(rootOrgMap);
  }

  private void mockContentStoreUtil() {
    Map<String, Object> contentMap = new HashMap<>();
    contentMap.put(JsonKey.RESPONSE, null);
    when(ContentStoreUtil.readFramework("invalidFrameworkId")).thenReturn(contentMap);
  }

  private void mockDatacacheHandler() {
    Map<String, List<String>> frameworkFieldsConfigMap = new HashMap<>();
    List<String> frameworkFieldConfig =
        Arrays.asList("id", "medium", "gradeLevel", "board", "subject");
    List<String> frameworkFieldConfigMan = Arrays.asList("id", "medium", "gradeLevel", "board");
    frameworkFieldsConfigMap.put(JsonKey.FIELDS, frameworkFieldConfig);
    frameworkFieldsConfigMap.put(JsonKey.MANDATORY_FIELDS, frameworkFieldConfigMan);
    Mockito.when(DataCacheHandler.getFrameworkFieldsConfig()).thenReturn(frameworkFieldsConfigMap);

    Map<String, List<Map<String, String>>> frameworkCategoriesMap = new HashMap<>();
    frameworkCategoriesMap.put("medium", getListForCategoryMap("English"));
    frameworkCategoriesMap.put("gradeLevel", getListForCategoryMap("Grade 3"));
    frameworkCategoriesMap.put("board", getListForCategoryMap("NCERT"));

    Map<String, Map<String, List<Map<String, String>>>> frameworkCategory = new HashMap<>();
    frameworkCategory.put("NCF", frameworkCategoriesMap);
    when(DataCacheHandler.getFrameworkCategoriesMap()).thenReturn(frameworkCategory);
    Map<String, List<String>> map1 = new HashMap<>();
    List<String> list1 = Arrays.asList("NCF");
    map1.put("someHashTagId", list1);
    when(DataCacheHandler.getHashtagIdFrameworkIdMap()).thenReturn(map1);
  }

  private Request getRequest(String key, String value) {
    Request reqObj = new Request();
    reqObj.getRequest().put(JsonKey.USER_ID, userId);
    reqObj.setOperation(ActorOperations.UPDATE_USER.getValue());
    Map<String, Object> innerMap = new HashMap<>();
    innerMap.put(JsonKey.ID, userId);
    Map<String, Object> frameworkMap = getFrameworkDetails(key, value);
    reqObj.getRequest().put(JsonKey.USER_ID, userId);
    innerMap.put(JsonKey.FRAMEWORK, frameworkMap);
    Map<String, Object> request = new HashMap<String, Object>();
    request.put(JsonKey.USER, innerMap);
    request.put(JsonKey.USER_ID, userId);
    request.put(JsonKey.FRAMEWORK, frameworkMap);
    reqObj.setRequest(request);
    Map<String, Object> context = new HashMap<>();
    context.put(JsonKey.REQUESTED_BY, "someValue");
    context.put(JsonKey.USER_ID, userId);
    reqObj.setContext(context);
    return reqObj;
  }

  private List<Map<String, String>> getListForCategoryMap(String value) {
    Map<String, String> map2 = new HashMap<>();
    map2.put(JsonKey.NAME, value);
    List<Map<String, String>> list2 = new ArrayList<>();
    list2.add(map2);
    return list2;
  }

  private Map<String, Object> getFrameworkDetails(String key, String value) {
    Map<String, Object> frameworkMap = new HashMap<>();
    List<String> medium = new ArrayList<>();
    medium.add("English");
    List<String> gradeLevel = new ArrayList<>();
    gradeLevel.add("Grade 3");
    List<String> board = new ArrayList<>();
    board.add("NCERT");
    frameworkMap.put(JsonKey.ID, "NCF");
    frameworkMap.put("gradeLevel", gradeLevel);
    frameworkMap.put("board", board);
    frameworkMap.put("medium", medium);
    if (key != null) {
      List<String> wrongValue = new ArrayList<>();
      wrongValue.add(value);
      frameworkMap.put(key, wrongValue);
    }
    return frameworkMap;
  }
}
