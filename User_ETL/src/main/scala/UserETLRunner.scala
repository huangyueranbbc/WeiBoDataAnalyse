import cn.hyr.user.etl.service.UserIdDuplicateRemovalService

/** *****************************************************************************
  * @date 2019-08-31 12:26
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  ******************************************************************************/
object UserETLRunner {


  def main(args: Array[String]): Unit = {
    // var etlType = args(0)
    var etlType="UserIdDuplicateRemoval"
    etlType match {
      case "UserIdDuplicateRemoval" => UserIdDuplicateRemovalService.run()
    }
  }

}
