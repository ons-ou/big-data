package kafka.sources;

import org.json.JSONObject;

import java.io.IOException;

public interface APISource<ResponseType> {

    ResponseType getResponse() throws IOException;
}
