package PICA::SOAPClient;
#ABSTRACT: PICA::Store via SOAP access (aka 'webcat')
use strict;
use warnings;

use PICA::Record;
use PICA::Store;
use SOAP::Lite; # +trace => 'debug';
use SOAP::Lite;
use Carp qw(croak);
use Cwd qw(cwd);
use Encode qw(encode_utf8);

our @ISA=qw(PICA::Store);

=head1 SYNOPSIS

  use PICA::SOAPClient;

  # connect to store via SOAP API
  $server = PICA::SOAPClient->new( 
      $baseurl, 
      userkey => $userkey, password => $password, dbsid => $dbsid 
  );

  # get connection details from config file
  $server = PICA::SOAPClient->new( config => "myconf.conf" );
  $server = PICA::SOAPClient->new( config => undef ); # autodetect (!)

  # CRUD operations
  %result = $server->get( $id );
  %result = $server->create( $record );
  %result = $server->update( $id, $record, $version );
  %result = $server->delete( $id );

  # set additional access parameters
  $store->access(
      userkey => $userkey, password => $password, dbsid => $dbsid
  );

=head1 DESCRIPTION

This class implements a L<PICA::Store> via SOAP-API (also know as 
"webcat"). A formal description of the CBS webcat SOAP-API can be
found at http://cws.gbv.de/ws/webcatws.wsdl.

=head1 METHODS

=head2 new ( %params )

Create a new Server. You must specify at least a connection type and a
base URL or the config file parameter to read this settings from a config
file. Defined parameters override settings in a config file.

Other parameters are userkey, password, and database id. The optional language 
parameter (default: "en") for error messagescan be one of "de", "en", "fr",
or "ne" depending in the servers capabilities.

Currently only the connection type "SOAP" is supported with limited error
handling.

=cut

sub new {
    my ($class) = shift;
    my ($soap, %params) = (@_ % 2) ? (@_) : (undef, @_);
    $params{SOAP} = $soap if defined $soap;

    PICA::Store::readconfigfile( \%params, $ENV{PICASTORE} )
        if exists $params{config} or exists $params{conf} ;

    croak "Missing SOAP base url (webcat)" unless defined $params{webcat};
    $params{dbsid} = "" unless defined $params{dbsid};
    $params{userkey} = "" unless defined $params{userkey};
    $params{language} = "en" unless defined $params{language};
    $params{password} = "" unless defined $params{password};

    $soap = SOAP::Lite->uri('http://www.gbv.de/schema/webcat-1.0')
                      ->proxy($params{webcat});
    # ->encoding('utf-8')
    # ->on_fault(sub{})

    my $self = bless {
        'soap' => $soap,
        'format' => SOAP::Data->name( "format" )->type( string => "pp" ),
        'rectype_title' => SOAP::Data->name( "rectype" )->type( string => "title" ),
        'rectype_entry' => SOAP::Data->name( "rectype" )->type( string => "entry" ),
        'baseurl' => $params{webcat},
    }, $class;

    return $self->access( %params );
}

=head2 get ( $id )

Retrieve a record by ID.

Returns a hash with either 'errorcode' and 'errormessage'
or a hash with 'id', 'record', and 'version'. The 'record'
element contains a L<PICA::Record> object.

=cut

sub get {
    my ($self, $id) = @_;
    my %result = $self->_soap_query( "get", 
        SOAP::Data->name( "ppn" )->type( string => $id )
    );
    $result{record} = PICA::Record->new($result{record}) if $result{record};
    return %result;
}

=head2 create ( $record )

Insert a new record. The parameter must be a L<PICA::Record> object.

Returns a hash with either 'errorcode' and 'errormessage' or a hash
with 'id', 'record', and 'version'.

=cut

sub create {
    my ($self, $record) = @_;
    croak('create needs a PICA::Record object') 
        unless UNIVERSAL::isa($record,'PICA::Record');
    my $rectype = $self->{"rectype_title"};

    my $sf = $record->subfield('002@$0');
    $rectype = $self->{"rectype_entry"} if ($sf && $sf =~ /^T/); # authority record

    my $recorddata = encode_utf8( $record->string );

    return $self->_soap_query( "create",
        SOAP::Data->name( "record" )->type( string => $recorddata ),
        $rectype
    );
}

=head2 update ( [ $id, ] $record [, $version ] )

Update a record by ID, updated record (of type L<PICA::Record>),
and version (of a previous get, create, or update command). If
no C<$id> parameter is given, it is taken from C<$record-E<gt>ppn>.

Returns a hash with either 'errorcode' and 'errormessage'
or a hash with 'id', 'record', and 'version'.

=cut

sub update {
    my ($self, $id, $record, $version) = @_;
    my $recorddata;
    if ( UNIVERSAL::isa( $id, 'PICA::Record' ) ) { # TODO: Test this
        $version = $record;
        $record = $id;
        $id = $id->ppn;
        $record->ppn( undef );
        $recorddata = encode_utf8( $record->string );
        $record->ppn($id);
    } else {
        croak('update needs an ID and a PICA::Record object')
            unless UNIVERSAL::isa ($record,'PICA::Record');
        $recorddata = encode_utf8( $record->string );
    }

    if (not defined $version) {
        my %current = $self->get( $id );
        return %current unless $current{version};
        $version = $current{version};
    }

    return $self->_soap_query( "update",
        SOAP::Data->name("ppn")->type( string => $id ),
        SOAP::Data->name("record")->type( string => $recorddata ),
        SOAP::Data->name("version")->type( string => $version )
    );
}

=head2 delete ( $id )

Deletes a record by ID.

Returns a hash with either 'errorcode' and 'errormessage' or a hash with 'id'.

=cut

sub delete {
    my ($self, $id) = @_;
    return $self->_soap_query( "delete", 
        SOAP::Data->name( "ppn" )->type( string  => $id )
    );
}

=head2 access ( key => value ... )

Set general access parameters (userkey, password, dbsid and/or language).
Returns the store itself so you can chain anothe method call.

=cut

sub access {
    my ($self, %params) = @_;

    for my $key (qw(userkey password dbsid language)) {
        $self->{$key} =
            SOAP::Data->name( $key => $params{$key} )->type('string')
            if defined $params{$key};
    }

    return $self;
}

=head2 about

Return a string "SOAPClient: $baseurl" with information about this store.

=cut

sub about {
    my $self = shift;
    return "CWS Webcat: " . $self->{baseurl};
}


=head1 INTERNAL METHODS

=head2 _soap_query ( $operation, @params )

Internal method to prepare, perform and evaluate a SOAP request. Returns
a hash with 'errorcode' and 'errormessage' or a hash with 'dbsid', 'id',
'record', and 'version' depending on the type of query. Do not directly
call this method!

=cut

sub _soap_query {
    my ($self, $operation, @params) = @_;

    push @params, $self->{"format"} unless $operation eq "delete"; 
    push @params, ($self->{dbsid}, $self->{userkey}, $self->{language});
    push @params, $self->{password} if defined $self->{password};

    my $response = $self->{soap}->$operation( @params );

    my %result;
    if (!$response) { 
        $result{errorcode}    = "1";
        $result{errormessage} = "No response to SOAP operation '$operation'.";
    } elsif ($response->fault) {
        $result{errorcode}    = $response->faultcode;
        $result{errormessage} = $response->faultstring;
        chomp $result{errormessage};
    } else {
        $result{id} = $response->valueof("//ppn") if defined $response->valueof("//ppn");
        $result{record} = PICA::Record->new( $response->valueof("//record") ) 
            if defined $response->valueof("//record");
        $result{version} = $response->valueof("//version")
            if defined $response->valueof("//version");
    }

    return %result;
}

1;

=encoding utf-8
