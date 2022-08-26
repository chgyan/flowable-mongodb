package org.flowable.mongodb.persistence.bean;

/**
 * @author chgyan
 */
public class RelationBean {

    private String relationCollection;

    private String localField;

    private String relationField;

    private String asName;


    public RelationBean() {
    }

    public RelationBean(String relationCollection, String localField, String relationField, String asName) {
        this.relationCollection = relationCollection;
        this.localField = localField;
        this.relationField = relationField;
        this.asName = asName;
    }

    public String getRelationCollection() {
        return relationCollection;
    }

    public void setRelationCollection(String relationCollection) {
        this.relationCollection = relationCollection;
    }

    public String getLocalField() {
        return localField;
    }

    public void setLocalField(String localField) {
        this.localField = localField;
    }

    public String getRelationField() {
        return relationField;
    }

    public void setRelationField(String relationField) {
        this.relationField = relationField;
    }

    public String getAsName() {
        return asName;
    }

    public void setAsName(String asName) {
        this.asName = asName;
    }


}
