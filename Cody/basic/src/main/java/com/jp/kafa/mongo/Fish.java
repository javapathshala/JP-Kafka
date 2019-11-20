/*
 * Copyright (c) KareXpert Technologies
 * Leading Healthcare Service Provider
 * All rights reserved.
 *
 * No parts of this source code can be reproduced without written
 * consent from KareXpert Technologies.
 * www.karexpert.com
 *
 */
package com.jp.kafa.mongo;

import org.bson.Document;

/**
 *
 *
 *
 * @author dimit
 * @since 1.0
 * @version 1.0
 */
public class Fish
{

    private int internationalFishId;
    private String name;
    private Breed breed;

    public enum Breed
    {
        Cod, Goldfish, Bass, Billy, Kipper, Turbot
    };

    public Fish(int internationalFishId, String name, Breed breed)
    {
        this.internationalFishId = internationalFishId;
        this.name = name;
        this.breed = breed;
    }

    public int getInternationalFishId()
    {
        return internationalFishId;
    }

    public void setInternationalFishId(int internationalFishId)
    {
        this.internationalFishId = internationalFishId;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public Breed getBreed()
    {
        return breed;
    }

    public String getBreedAsString()
    {
        String breedString;
        switch (breed)
        {
            case Cod:
                breedString = "Cod";
                break;
            case Goldfish:
                breedString = "Goldfish";
                break;
            case Bass:
                breedString = "Bass";
                break;
            case Billy:
                breedString = "Billy";
                break;
            case Kipper:
                breedString = "Kipper";
                break;
            case Turbot:
                breedString = "Turbot";
                break;
            default:
                breedString = "Unknown breed";
                break;
        }
        return breedString;
    }

    ;
    public void setBreed(Breed breed)
    {
        this.breed = breed;
    }

    public Document getFishAsDocument()
    {
        Document fishDocument = new Document("_id", getInternationalFishId())
                .append("name", getName())
                .append("breed", getBreedAsString());
        return fishDocument;
    }

    ;

    @Override
    public String toString()
    {
        return "Fish Object: {"
                + "internationalFishId=" + internationalFishId
                + ", name='" + name + '\''
                + ", breed=" + breed
                + '}';
    }
}
