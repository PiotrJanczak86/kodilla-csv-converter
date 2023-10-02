package com.kodilla.csvconverter;

import org.springframework.batch.item.ItemProcessor;

import java.time.Year;

public class AgeProcessor implements ItemProcessor<PersonWithBirthYear, PersonWithAge> {

    @Override
    public PersonWithAge process(PersonWithBirthYear item){
        return new PersonWithAge(item.getName(), item.getSurname(), Year.now().getValue() - item.getBirthYeah());
    }
}