package tsv2xes;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
//	private static Pattern AOIpattern = Pattern.compile("AOI\\[([\\w\\s\\d]+)\\]Hit");
	private static Pattern AOIpattern = Pattern.compile("AOI\\[([Q\\d]+)\\]Hit");

	public static void main(String[] args) throws IOException, ParseException {
		
		String inFile = "C:\\Users\\andbur\\Desktop\\1.1pm.tsv";
		String outFile = "C:\\Users\\andbur\\Desktop\\1.1pm.tsv.xes";
		
		XLog log = XLogHelper.generateNewXLog("from tsv file");
		Map<String, XTrace> studentsToTraces = new HashMap<String, XTrace>();
		
		Reader reader = new InputStreamReader(new BOMInputStream(new FileInputStream(inFile)), "UTF-8");
		CSVParser parser = new CSVParser(reader, CSVFormat.TDF.withHeader());
		Map<String, String> AOIfields = new HashMap<String, String>();
		
		int sec = 0;
		for (CSVRecord record : parser) {
			// identify all aoi fields from headers
			if (AOIfields.isEmpty()) {
				for (String header : parser.getHeaderMap().keySet()) {
					Matcher matcher = AOIpattern.matcher(header);
					if (matcher.find()) {
						AOIfields.put(header, matcher.group(1));
					}
				}
			}
			
			String participantName = record.get("ParticipantName");
			Date startDate = dateParser.parse(record.get("LocalTimeStamp"));
			Date endDate = new Date(startDate.getTime() + Integer.parseInt(record.get("GazeEventDuration")));
			
			XTrace currentSubjectTrace = studentsToTraces.get(participantName);
			if (currentSubjectTrace == null) {
				currentSubjectTrace = XLogHelper.insertTrace(log, participantName);
				studentsToTraces.put(participantName, currentSubjectTrace);
				sec = 0;
			}
			
			for (String AOIField : AOIfields.keySet()) {
				if (record.get(AOIField).equals("1")) {
	//				Date startDateShifted = new Date(startDate.getTime() + 1000 * (sec++));
					XEvent eventStart = XLogHelper.insertEvent(currentSubjectTrace, AOIfields.get(AOIField), startDate);
					XLogHelper.decorateElement(eventStart, "lifecycle:transition", "start", "Lifecycle");
//					XEvent eventComplete = XLogHelper.insertEvent(currentSubjectTrace, AOIfields.get(AOIField), endDate);
//					XLogHelper.decorateElement(eventComplete, "lifecycle:transition", "complete", "Lifecycle");
				}
			}
		}
		parser.close();
		
		XSerializer serializer = new XesXmlSerializer(); //XesXmlGZIPSerializer();
		serializer.serialize(log, new FileOutputStream(outFile));
		System.out.println("Done");
	}
}


