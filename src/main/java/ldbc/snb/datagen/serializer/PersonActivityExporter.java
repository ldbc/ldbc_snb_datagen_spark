package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.util.FactorTable;

import java.io.IOException;

/**
 * Created by aprat on 3/01/16.
 */
public class PersonActivityExporter {
    protected PersonActivitySerializer personActivitySerializer_;
    protected UpdateEventSerializer updateSerializer_;
    protected FactorTable factorTable_;

    public PersonActivityExporter(PersonActivitySerializer personActivitySerializer, UpdateEventSerializer updateEventSerializer, FactorTable factorTable) {
        this.personActivitySerializer_ = personActivitySerializer;
        this.updateSerializer_ = updateEventSerializer;
        this.factorTable_ = factorTable;
    }

        public void export(final Forum forum) throws IOException {
            if(forum.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
                personActivitySerializer_.export(forum);
            } else {
                updateSerializer_.export(forum);
            }
        }

        public void export(final Post post) throws IOException {
            if(post.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
                personActivitySerializer_.export(post);
                factorTable_.extractFactors(post);
            } else {
                updateSerializer_.export(post);
            }
        }

        public void export(final Comment comment) throws IOException {
            if(comment.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
                personActivitySerializer_.export(comment);
                factorTable_.extractFactors(comment);
            } else {
                updateSerializer_.export(comment);
            }
        }

        public void export(final Photo photo) throws IOException {
            if(photo.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
                personActivitySerializer_.export(photo);
                factorTable_.extractFactors(photo);
            } else {
                updateSerializer_.export(photo);
            }
        }

        public void export(final ForumMembership member) throws IOException {
            if(member.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
                personActivitySerializer_.export(member);
                factorTable_.extractFactors(member);
            } else {
                updateSerializer_.export(member);
            }
        }

        public void export(final Like like) throws IOException {
            if(like.date < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
                personActivitySerializer_.export(like);
                factorTable_.extractFactors(like);
            } else {
                updateSerializer_.export(like);
            }
        }
    }
