package org.sunbird.user;

import static akka.testkit.JavaTestKit.duration;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import java.util.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.cassandraimpl.CassandraOperationImpl;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.request.Request;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.user.actors.AddressManagementActor;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
  ServiceFactory.class,
  ElasticSearchUtil.class,
  EncryptionService.class,
  org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.class
})
@PowerMockIgnore({"javax.management.*"})
public class AddressManagementActorTest {

  private static ActorSystem system = ActorSystem.create("system");;
  private static final Props props = Props.create(AddressManagementActor.class);
  private CassandraOperation cassandraOperation;

  @BeforeClass
  public static void setup() {
    //    system = ActorSystem.create("system");
    PowerMockito.mockStatic(ServiceFactory.class);
  }

  @Before
  public void beforeEachTest() throws Exception {

    PowerMockito.mockStatic(org.sunbird.common.models.util.datasecurity.impl.ServiceFactory.class);
    EncryptionService encryptionService = Mockito.mock(EncryptionService.class);
    Mockito.when(
            org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
                .getEncryptionServiceInstance(null))
        .thenReturn(encryptionService);

    try {
      Mockito.when(encryptionService.encryptData(Mockito.anyString())).thenReturn("abc123");
    } catch (Exception e) { // TODO Auto-generated catch block
      Assert.fail("Initialization failed");
    }

    cassandraOperation = mock(CassandraOperationImpl.class);
    when(ServiceFactory.getInstance()).thenReturn(cassandraOperation);
  }

  @Test
  public void testInsertAddress() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation("insertUserAddress");
    reqObj.put(JsonKey.ADDRESS, getAddressList());
    reqObj.put(JsonKey.ID, "someId");
    reqObj.put(JsonKey.CREATED_BY, "createdBy");

    //        when(cassandraOperation.getRecordsByProperties(
    //                Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
    //                .thenReturn(getSuccessResponse());
    //        when(ElasticSearchUtil.upsertData(
    //                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
    // Mockito.anyMap()))
    //                .thenReturn(true);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(true == true);
  }

  @Test
  public void testUpdateAddress() {
    TestKit probe = new TestKit(system);
    ActorRef subject = system.actorOf(props);

    Request reqObj = new Request();
    reqObj.setOperation("updateUserAddress");
    reqObj.put(JsonKey.ADDRESS, getAddressList());
    reqObj.put(JsonKey.ID, "someId");
    reqObj.put(JsonKey.CREATED_BY, "createdBy");

    when(cassandraOperation.deleteRecord(
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(getSuccessResponse());

    //        when(ElasticSearchUtil.upsertData(
    //                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
    // Mockito.anyMap()))
    //                .thenReturn(true);

    subject.tell(reqObj, probe.getRef());
    Response res = probe.expectMsgClass(duration("1000 second"), Response.class);
    Assert.assertTrue(true == true);
  }

  private Object getAddressList() {

    List<Map<String, Object>> lst = new ArrayList<>();
    Map<String, Object> map = new HashMap<>();
    map.put(JsonKey.ADDRESS, "anyAddress");
    map.put(JsonKey.IS_DELETED, true);
    lst.add(map);
    return lst;
  }

  private Response getSuccessResponse() {
    Response response = new Response();
    response.put(JsonKey.RESPONSE, "SUCCESS");
    return response;
  }
}