package me.mircea.licenta.manager;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import me.mircea.licenta.core.SecretManager;
import me.mircea.licenta.core.crawl.db.CrawlDatabaseManager;
import me.mircea.licenta.core.crawl.db.model.Job;
import me.mircea.licenta.core.crawl.db.model.Session;
import me.mircea.licenta.core.crawl.db.model.Site;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

@Path("/sessions")
public class SessionResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionResource.class);
    private Client httpClient = ClientBuilder.newClient();

    @GET
    @Path("{sessionId}")
    public Session getSessionStatus(@PathParam("sessionId") ObjectId sessionId) {
        //return CrawlDatabaseManager.instance.getSessionById(sessionId);
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @GET
    @Path("/print")
    @Produces(MediaType.APPLICATION_JSON)
    public String printSomething() {
        return "Echo echo.. check";
    }

    @POST
    @Path("/crawl")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, ObjectId> startCrawlSession() {
        return startSession(SecretManager.instance.getCrawlerEndpoint());
    }

    @POST
    @Path("/scrape")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, ObjectId> startScrapeSession() {
        LOGGER.error("Starting to scrape...");
        return startSession(SecretManager.instance.getScraperEndpoint());
    }

    private Map<String, ObjectId> startSession(String endpoint) {
        Map<String, ObjectId> startedJobs = new HashMap<>();
        for (Site site : CrawlDatabaseManager.instance.getAllSites()) {
            if (!CrawlDatabaseManager.instance.isThereAnyJobRunningOnDomain(site)) {

                ObjectNode crawlRequestBody = new ObjectNode(JsonNodeFactory.instance)
                        .put("homepage", site.getHomepage());
                ArrayNode seedList = crawlRequestBody.putArray("seeds");
                for (String seed : site.getSeeds())
                    seedList.add(seed);

                WebTarget crawlerTarget = httpClient.target(endpoint);
                Invocation.Builder invocationBuilder = crawlerTarget.path("/jobs")
                        .request(MediaType.APPLICATION_JSON);

                Response response = invocationBuilder.post(Entity.json(crawlRequestBody));

                if (response.getStatus() == 202) {
                    Job responseJob = response.readEntity(Job.class);
                    startedJobs.put(site.getDomain(), responseJob.getId());
                }
            } else {
                startedJobs.put(site.getDomain(), null);
            }
        }
        return startedJobs;
    }

}
