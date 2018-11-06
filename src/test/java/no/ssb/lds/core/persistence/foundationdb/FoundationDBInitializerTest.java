package no.ssb.lds.core.persistence.foundationdb;

import no.ssb.lds.api.persistence.Document;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class FoundationDBInitializerTest {

    public static void main(String[] args) {
        String namespace = "sample-namespace-1";
        FoundationDBPersistence persistence = new FoundationDBInitializer().initialize(
                namespace,
                Map.of("node-prefix.hex", "3A",
                        "content-prefix.hex", "3B"),
                Set.of("Person", "Address", "FunkyLongAddress"));
        ZonedDateTime jan1624 = ZonedDateTime.of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan1626 = ZonedDateTime.of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan1663 = ZonedDateTime.of(1663, 1, 1, 0, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime feb1663 = ZonedDateTime.of(1663, 2, 1, 0, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime mar1663 = ZonedDateTime.of(1663, 3, 1, 0, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan1664 = ZonedDateTime.of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
        ZonedDateTime sep94 = ZonedDateTime.of(1994, 9, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan00 = ZonedDateTime.of(2000, 1, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
        ZonedDateTime apr09 = ZonedDateTime.of(2009, 4, 2, 14, 44, 21, (int) TimeUnit.MILLISECONDS.toNanos(302), ZoneId.of("Etc/UTC"));
        ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
        ZonedDateTime dec11 = ZonedDateTime.of(2011, 12, 4, 16, 46, 23, (int) TimeUnit.MILLISECONDS.toNanos(304), ZoneId.of("Etc/UTC"));
        ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
        ZonedDateTime sep18 = ZonedDateTime.of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
        ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        ZonedDateTime oneMillisecondAfterNow = now.plus(1, ChronoUnit.MILLIS);
        persistence.createOrOverwrite(toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624));
        persistence.createOrOverwrite(toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626));
        persistence.createOrOverwrite(toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664));
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92));
        persistence.createOrOverwrite(toDocument(namespace, "Person", "jane", createPerson("Jane", "Doe"), sep94));
        persistence.createOrOverwrite(toDocument(namespace, "Person", "jane", createPerson("Jane", "Smith"), feb10));
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13));
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18));
        System.out.println();
        System.out.println("January 2000");
        System.out.format("john:    %s%n", persistence.read(jan00, namespace, "Person", "john"));
        System.out.format("jane:    %s%n", persistence.read(jan00, namespace, "Person", "jane"));
        System.out.format("newyork: %s%n", persistence.read(jan00, namespace, "Address", "newyork"));
        System.out.println();
        System.out.println("April 2009");
        System.out.format("john:    %s%n", persistence.read(apr09, namespace, "Person", "john"));
        System.out.format("jane:    %s%n", persistence.read(apr09, namespace, "Person", "jane"));
        System.out.format("newyork: %s%n", persistence.read(apr09, namespace, "Address", "newyork"));
        System.out.println();
        System.out.println("December 2011");
        System.out.format("john:    %s%n", persistence.read(dec11, namespace, "Person", "john"));
        System.out.format("jane:    %s%n", persistence.read(dec11, namespace, "Person", "jane"));
        System.out.format("newyork: %s%n", persistence.read(dec11, namespace, "Address", "newyork"));
        System.out.println();
        System.out.println("September 2018");
        System.out.format("john:    %s%n", persistence.read(sep18, namespace, "Person", "john"));
        System.out.format("jane:    %s%n", persistence.read(sep18, namespace, "Person", "jane"));
        System.out.format("newyork: %s%n", persistence.read(sep18, namespace, "Address", "newyork"));
        System.out.println();
        System.out.println("Now");
        System.out.format("john:    %s%n", persistence.read(now, namespace, "Person", "john"));
        System.out.format("jane:    %s%n", persistence.read(now, namespace, "Person", "jane"));
        System.out.format("newyork: %s%n", persistence.read(now, namespace, "Address", "newyork"));
        System.out.println();
        System.out.println("Now + 1ms");
        System.out.format("john:    %s%n", persistence.read(oneMillisecondAfterNow, namespace, "Person", "john"));
        System.out.format("jane:    %s%n", persistence.read(oneMillisecondAfterNow, namespace, "Person", "jane"));
        System.out.format("newyork: %s%n", persistence.read(oneMillisecondAfterNow, namespace, "Address", "newyork"));

        System.out.println();
        System.out.println("ALL newyork");
        for (Document document : persistence.readAllVersions(namespace, "Address", "newyork", 100)) {
            System.out.println(document);
        }

        System.out.println();
        System.out.println("ALL john");
        for (Document document : persistence.readAllVersions(namespace, "Person", "john", 100)) {
            System.out.println(document);
        }

        System.out.println();
        System.out.println("ALL jane");
        for (Document document : persistence.readAllVersions(namespace, "Person", "jane", 100)) {
            System.out.println(document);
        }

        System.out.println();
        System.out.format("John from %s to %s%n", feb10, sep18);
        for (Document document : persistence.readVersions(feb10, sep18, namespace, "Person", "john", 100)) {
            System.out.println(document);
        }

        System.out.println();
        System.out.format("All Persons with lastname Smith at %s%n", sep18);
        for (Document document : persistence.find(sep18, namespace, "Person", "lastname", "Smith", 100)) {
            System.out.println(document);
        }

        System.out.println();
        System.out.format("All Persons at %s%n", dec11);
        for (Document document : persistence.findAll(dec11, namespace, "Person", 100)) {
            System.out.println(document);
        }

        String bigString = "12345678901234567890";
        for (int i = 0; i < 12; i++) {
            bigString = bigString + "_" + bigString;
        }
        persistence.createOrOverwrite(toDocument(namespace, "FunkyLongAddress", "newyork", createAddress(bigString, "NY", "USA"), oct18));
        List<Document> shouldMatch = persistence.find(now, namespace, "FunkyLongAddress", "city", bigString, 100);
        if (shouldMatch.size() != 1) {
            throw new IllegalStateException("Test failed! " + shouldMatch.size());
        }
        List<Document> shouldNotMatch = persistence.find(now, namespace, "FunkyLongAddress", "city", bigString + "1", 100);
        if (shouldNotMatch.size() != 0) {
            throw new IllegalStateException("Test failed! " + shouldNotMatch.size());
        }
        persistence.delete(oct18, namespace, "FunkyLongAddress", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS);

        System.out.println();
        System.out.println("STATE BEFORE DELETE MARKER");
        System.out.format("newyork jan 1663:      %s%n", persistence.read(jan1663, namespace, "Address", "newyork"));
        System.out.format("newyork end jan 1663:  %s%n", persistence.read(feb1663.minus(1, ChronoUnit.MILLIS), namespace, "Address", "newyork"));
        System.out.format("newyork feb 1663:      %s%n", persistence.read(feb1663, namespace, "Address", "newyork"));
        System.out.format("newyork tick feb 1663: %s%n", persistence.read(feb1663.plus(1, ChronoUnit.MILLIS), namespace, "Address", "newyork"));
        System.out.format("newyork mar 1663:      %s%n", persistence.read(mar1663, namespace, "Address", "newyork"));
        System.out.format("newyork now     :      %s%n", persistence.read(now, namespace, "Address", "newyork"));
        System.out.println();
        System.out.println("Marking newyork as deleted as of feb 1663");

        persistence.markDeleted(feb1663, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS);

        System.out.println();
        System.out.println("STATE AFTER DELETE MARKER");
        System.out.format("newyork jan 1663:      %s%n", persistence.read(jan1663, namespace, "Address", "newyork"));
        System.out.format("newyork end jan 1663:  %s%n", persistence.read(feb1663.minus(1, ChronoUnit.MILLIS), namespace, "Address", "newyork"));
        System.out.format("newyork feb 1663:      %s%n", persistence.read(feb1663, namespace, "Address", "newyork"));
        System.out.format("newyork tick feb 1663: %s%n", persistence.read(feb1663.plus(1, ChronoUnit.MILLIS), namespace, "Address", "newyork"));
        System.out.format("newyork mar 1663:      %s%n", persistence.read(mar1663, namespace, "Address", "newyork"));
        System.out.format("newyork now     :      %s%n", persistence.read(now, namespace, "Address", "newyork"));

        persistence.close();
    }

    static JSONObject createPerson(String firstname, String lastname) {
        JSONObject person = new JSONObject();
        person.put("firstname", firstname);
        person.put("lastname", lastname);
        return person;
    }

    static JSONObject createAddress(String city, String state, String country) {
        JSONObject address = new JSONObject();
        address.put("city", city);
        address.put("state", state);
        address.put("country", country);
        return address;
    }

    static Document toDocument(String namespace, String entity, String id, JSONObject json, ZonedDateTime timestamp) {
        List<Fragment> fragments = new ArrayList<>();
        addFragments("$.", json, fragments);
        return new Document(namespace, entity, id, timestamp, fragments, false);
    }

    private static void addFragments(String pathPrefix, JSONObject json, List<Fragment> fragments) {
        for (Map.Entry<String, Object> entry : json.toMap().entrySet()) {
            String key = entry.getKey();
            Object untypedValue = entry.getValue();
            if (JSONObject.NULL.equals(untypedValue)) {
            } else if (untypedValue instanceof JSONObject) {
                addFragments(pathPrefix + "." + key, (JSONObject) untypedValue, fragments);
            } else if (untypedValue instanceof JSONArray) {
            } else if (untypedValue instanceof String) {
                String value = (String) untypedValue;
                fragments.add(new Fragment(key, value));
            } else if (untypedValue instanceof Number) {
            } else if (untypedValue instanceof Boolean) {
            }
        }
    }
}
