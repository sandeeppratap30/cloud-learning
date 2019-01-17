package com.journaldev.spring;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.beans.factory.annotation.Autowired;

@RestController
public class PersonController {
	
	public Logger logger = Logger.getLogger(PersonController.class);

        private PersonGoogleCloudDatastore dataStore = new PersonGoogleCloudDatastore();
	
	public PersonController() {
	}
	@RequestMapping("/")
	public String healthCheck() {
		return "OK";
	}
	
	@RequestMapping("/person/get")
	public List<String> getPerson(@RequestParam(name="name", required=false, defaultValue="Unknown") String name) {
		 List<String> persons = dataStore.formatPersons(dataStore.listPersons());
	        logger.info("found persons : " + persons.size());
	        logger.info("Person ID : Name");
	        logger.info("---------------------");
	        for (String person : persons) {
	        	logger.info(person);
	        }
		return persons;
	}
		
	@RequestMapping(value="/person/update", method=RequestMethod.POST, consumes = "application/json")
	public Person updatePerson(@RequestBody Person p) {
		dataStore.addPerson(p.getName());
		return p;
	}
}
