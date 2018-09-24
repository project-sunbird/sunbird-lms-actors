package org.sunbird.learner.actors.coursebatch.dao;

import java.util.List;
import java.util.Map;
import org.sunbird.common.models.response.Response;
import org.sunbird.models.course.batch.CourseBatch;

/** Created by rajatgupta on 12/09/18. */
public interface CourseBatchDao {

  /**
   * Create course batch.
   *
   * @param map Create course batch map
   * @return Response containing setting identifier.
   */
  Response create(Map<String, Object> map);

  /**
   * Update course batch.
   *
   * @param map Create course batch map
   * @return Response containing setting identifier.
   */
  Response update(Map<String, Object> map);

  /**
   * Read course batch for given identifier.
   *
   * @param id fetch course batch using id
   * @return course batch information
   */
  CourseBatch readById(String id);

  /**
   * Read all course batch.
   *
   * @return Response containing operation is success or not.
   */
  Response delete(String id);

  List<Map<String, Object>> readPublishedCourse(String id);

  Response createCourseEnrolment(Map<String, Object> map);
}
