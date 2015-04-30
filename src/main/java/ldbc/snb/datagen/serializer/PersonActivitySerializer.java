/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.objects.*;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author aprat
 */
abstract public class PersonActivitySerializer {


public PersonActivitySerializer() {
}

public void export( Forum forum ) {
	serialize(forum);
}

public void export( ForumMembership forumMembership ) {
		serialize(forumMembership);
}

public void export( Post post ) {
		serialize(post);
}

public void export( Comment comment ) {
	serialize(comment);

}

public void export( Photo photo ) {
	serialize(photo);

}
	
public void export( Like like ) {
	serialize(like);

}


abstract public void reset();

abstract public void initialize(Configuration conf, int reducerId);

abstract public void close();

abstract protected void serialize( Forum forum );

abstract protected void serialize( Post post );

abstract protected void serialize( Comment comment );

abstract protected void serialize( Photo photo );
	
abstract protected void serialize( ForumMembership membership );

abstract protected void serialize( Like like );


}
