package sgtransit.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class ArrivalProducerTest {

    @Test
    void parseStopsTrimsAndDropsDuplicates() {
        assertEquals(List.of("14031", "14039"), ArrivalProducer.parseStops(" 14031, 14039,,14031 "));
    }

    @Test
    void parseStopsRejectsInvalidCodes() {
        assertThrows(IllegalArgumentException.class, () -> ArrivalProducer.parseStops("14031,1403"));
        assertThrows(IllegalArgumentException.class, () -> ArrivalProducer.parseStops("14031,abcde"));
    }

    @Test
    void parseStopsRejectsEmptyList() {
        assertThrows(IllegalArgumentException.class, () -> ArrivalProducer.parseStops(" , "));
    }

    @Test
    void toMessageKeepsResponseAndAddsPolledAt() {
        String body = "{\"BusStopCode\":\"14031\",\"Services\":[{\"ServiceNo\":\"65\"}]}";
        JsonNode msg = JsonMapper.shared().readTree(
                ArrivalProducer.toMessage(body, Instant.parse("2026-10-23T06:00:00Z")));

        assertEquals("14031", msg.get("BusStopCode").asString());
        assertEquals("65", msg.get("Services").get(0).get("ServiceNo").asString());
        assertEquals("2026-10-23T06:00:00Z", msg.get("polled_at").asString());
    }

    @Test
    void toMessageRejectsNonObjectBody() {
        assertThrows(IllegalArgumentException.class, () -> ArrivalProducer.toMessage("[]", Instant.now()));
    }
}
