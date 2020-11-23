import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/22 22:36
 */
public class JSONTest {

    public static void main(String[] args) {
        String jsonData = "{\n" +
                "    \"time\": \"1\",\n" +
                "    \"title\": \"2\",\n" +
                "    \"score\": \"3\",\n" +
                "    \"awaiting\": \"4\",\n" +
                "    \"watching\": \"5\",\n" +
                "    \"seen\": \"6\",\n" +
                "    \"short_comment_count\": \"7\",\n" +
                "    \"short_comment_like_count\": \"8\",\n" +
                "    \"comment_count\": \"9\",\n" +
                "    \"comment_reply_count\": \"10\",\n" +
                "    \"comment_usefulness_count\": \"11\",\n" +
                "    \"comment_share_count\": \"12\",\n" +
                "    \"comment_collect_count\": \"13\"\n" +
                "}";
        JSONObject jsonObject = JSON.parseObject(jsonData);
        for (String key : jsonObject.keySet()) {
            String value = jsonObject.getString(key);
            System.out.println(key + " - " + value);
        }

        JSONObject json = new JSONObject();
        json.put("hello", 1);
        json.put("world", 2);
        json.put("again", "Hello world!");
        System.out.println(json.toJSONString());

        System.out.println(JSON.parseObject(jsonData).getString("title"));
    }
}
