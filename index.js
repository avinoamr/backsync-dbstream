var backsync = require( "backsync" );
module.exports = function ( connection ) {
    var Cursor = connection.Cursor;

    var update = function ( model, options, callback ) {
        var obj = ( model.toJSON ) ? model.toJSON() : model;
        var cursor = new Cursor();
        cursor.once( "error", callback );
        cursor.once( "finish", function () {
            callback( null, obj );
        });

        ( !options.remove ) 
            ? cursor.write( obj ) 
            : cursor.remove({ id: model.id });

        cursor.end();
    }

    var read = function ( model, options, callback ) {
        new Cursor()
            .find({ id: model.id })
            .limit( 1 )
            .once( "error", callback )
            .once( "data", function ( obj ) {
                callback( null, obj );
                callback = null;
            })
            .once( "end", function () {
                if ( callback ) {
                    callback( new backsync.NotFoundError( model.id ) );
                }
            });
    }


    return backsync()
        .create( update )
        .update( update )
        .read( read )
        .patch( function ( model, options, callback ) {
            read( model, { silent: true }, function ( err, obj ) {
                if ( err ) return callback( err );
                obj = extend( {}, obj, options.attrs );
                update( obj, {}, callback );
            });
        })
        .search( function ( collection, options, callback ) {
            options.data || ( options.data = {} );
            var data = extend( {}, options.data || {} );
            delete data[ "$sort" ];
            delete data[ "$skip" ];
            delete data[ "$limit" ];

            var results = [];
            var cursor = new Cursor()
                .find( data )
                .sort( options.data.$sort || null )
                .skip( options.data.$skip || 0 )
                .limit( options.data.$limit || Infinity )
                .once( "error", callback )
                .once( "end", function() { callback( null, results ); })
                .on( "data", function ( obj ) { results.push( obj ) } )
        })
        .delete( function ( model, options, callback ) {
            update( model, extend({ remove: true }, options ), callback );
        });
}

function extend ( obj ) {
    [].slice.call(arguments, 1).forEach(function(source) {
      if (source) {
        for (var prop in source) {
          obj[prop] = source[prop];
        }
      }
    });
    return obj;
}
