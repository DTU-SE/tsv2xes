package tsv2xes;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.out.XSerializer;
import org.deckfour.xes.out.XesXmlSerializer;

import tsv2xes.utils.XLogHelper;


public class tsv2xes {

	private static SimpleDateFormat dateParser = new SimpleDateFormat("HH:mm:ss.SSS");
	private static Pattern AOIpatternsQuadrants = Pattern.compile("AOI\\[([Q\\d]+|legend|title)\\]Hit");
	private static Pattern AOIpatternsGraph = Pattern.compile("AOI\\[(graph|legend|title)\\]Hit");
	private static boolean generateEndEvent = true;

	public static void main(String[] args) throws IOException, ParseException {
		
		String inFile = "C:\\Users\\andbur\\Desktop\\1.1pm.tsv";
		
		XLog intermediateLogQuadrants = XLogHelper.generateNewXLog("Quadrants");
		XLog intermediateLogGraph = XLogHelper.generateNewXLog("Graph");
		Map<String, XTrace> studentsToQuadrantsTraces = new HashMap<String, XTrace>();
		Map<String, XTrace> studentsToGraphTraces = new HashMap<String, XTrace>();
		
		Reader reader = new InputStreamReader(new BOMInputStream(new FileInputStream(inFile)), "UTF-8");
		CSVParser parser = new CSVParser(reader, CSVFormat.TDF.withHeader());
		Map<String, String> AOIfieldsQuadrants = new HashMap<String, String>();
		Map<String, String> AOIfieldsGraph = new HashMap<String, String>();
		
		for (CSVRecord record : parser) {
			// identify all aoi fields from headers
			if (AOIfieldsQuadrants.isEmpty()) {
				for (String header : parser.getHeaderMap().keySet()) {
					Matcher matcher = AOIpatternsQuadrants.matcher(header);
					if (matcher.find()) {
						AOIfieldsQuadrants.put(header, matcher.group(1));
					}
				}
			}
			if (AOIfieldsGraph.isEmpty()) {
				for (String header : parser.getHeaderMap().keySet()) {
					Matcher matcher = AOIpatternsGraph.matcher(header);
					if (matcher.find()) {
						AOIfieldsGraph.put(header, matcher.group(1));
					}
				}
			}
			
			String participantName = record.get("ParticipantName");
			Date startDate = dateParser.parse(record.get("LocalTimeStamp"));
			Date endDate = new Date(startDate.getTime() + Integer.parseInt(record.get("GazeEventDuration")));
			
			// add aois referring to quadrants
			XTrace currentSubjectTraceQuadrants = studentsToQuadrantsTraces.get(participantName);
			if (currentSubjectTraceQuadrants == null) {
				currentSubjectTraceQuadrants = XLogHelper.insertTrace(intermediateLogQuadrants, participantName);
				studentsToQuadrantsTraces.put(participantName, currentSubjectTraceQuadrants);
			}
			for (String AOIField : AOIfieldsQuadrants.keySet()) {
				if (record.get(AOIField).equals("1")) {
					XEvent eventStart = XLogHelper.insertEvent(currentSubjectTraceQuadrants, AOIfieldsQuadrants.get(AOIField), startDate);
					XLogHelper.decorateElement(eventStart, "lifecycle:transition", "start", "Lifecycle");
					if (generateEndEvent) {
						XEvent eventComplete = XLogHelper.insertEvent(currentSubjectTraceQuadrants, AOIfieldsQuadrants.get(AOIField), endDate);
						XLogHelper.decorateElement(eventComplete, "lifecycle:transition", "complete", "Lifecycle");
					}
				}
			}
			
			// add aois referring to graph
			XTrace currentSubjectTraceGraph = studentsToGraphTraces.get(participantName);
			if (currentSubjectTraceGraph == null) {
				currentSubjectTraceGraph = XLogHelper.insertTrace(intermediateLogGraph, participantName);
				studentsToGraphTraces.put(participantName, currentSubjectTraceGraph);
			}
			for (String AOIField : AOIfieldsGraph.keySet()) {
				if (record.get(AOIField).equals("1")) {
					XEvent eventStart = XLogHelper.insertEvent(currentSubjectTraceGraph, AOIfieldsGraph.get(AOIField), startDate);
					XLogHelper.decorateElement(eventStart, "lifecycle:transition", "start", "Lifecycle");
					if (generateEndEvent) {
						XEvent eventComplete = XLogHelper.insertEvent(currentSubjectTraceGraph, AOIfieldsGraph.get(AOIField), endDate);
						XLogHelper.decorateElement(eventComplete, "lifecycle:transition", "complete", "Lifecycle");
					}
				}
			}
			
		}
		parser.close();
		
		XSerializer serializer = new XesXmlSerializer(); //XesXmlGZIPSerializer();
		// export quadrant log
		serializer.serialize(XLogHelper.mergeEventsWithSameName(intermediateLogQuadrants, generateEndEvent), new FileOutputStream(inFile + "-quadrants.xes"));
		// export graph log
		serializer.serialize(XLogHelper.mergeEventsWithSameName(intermediateLogGraph, generateEndEvent), new FileOutputStream(inFile + "-graph.xes"));
		
		System.out.println("Done");
	}
}


