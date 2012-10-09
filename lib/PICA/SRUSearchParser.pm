package PICA::SRUSearchParser;
#ABSTRACT: Parse a SRU response in XML and extract PICA+ records.
use strict;

=head1 SYNOPSIS

    $parser = PICA::SRUSearchParser->new();
    $xmlparser = $parser->parse( $sru );

    print "numberOfRecords: " . $parser->numberOfRecords . "\n";
    print "resultSetId: " . $parser->resultSetId . "\n";
    print "result: " . $xmlparser->counter() . "\n";

=cut

use Carp qw(croak);
use PICA::XMLParser;
use XML::SAX::ParserFactory;
use base qw(XML::SAX::Base);

=head1 METHODS

=head2 new ( [ $xmlparser ] )

Creates a new XML parser to parse an SRU Search Response document.
PICA Records are passed to a L<PICA::XMLParser> that must be provided.

=cut

sub new {
    my ($class, $xmlparser) = @_;
    $class = ref $class || $class;

    $xmlparser = PICA::XMLParser->new()
        unless UNIVERSAL::isa($xmlparser, "PICA::XMLParser");

    my $self = {
        xmlparser => $xmlparser,
        char_data => "",
        in_record => 0,
        numberOfRecords => undef,
        currentNumber => 0,
        resultSetId => undef,
    };

    return bless $self, $class;
}

=head2 parse( $document )

Parse an SRU SearchRetrieve Response (given as XML document)
and return the L<PICA::XMLParser> object that has been used.

=cut

sub parse {
    my ($self, $document) = @_;

    my $parser = XML::SAX::ParserFactory->new(
        RequiredFeatures => { 'http://xml.org/sax/features/namespaces' => 1 }
      )->parser( Handler => $self );

    $self->{currentNumber} = 0;
    $parser->parse_string($document);

    return $self->{xmlparser};
}

=head2 numberOfRecords ()

Get the total number of records in the SRU result set.
The result set may be split into several chunks.

=cut

sub numberOfRecords {
    my $self = shift;
    return $self->{numberOfRecords};
}

=head2 currentNumber ()

Get the current number of records that has been passed.
This is equal to or less then numberOfRecords.

=cut

sub currentNumber {
    my $self = shift;
    return $self->{currentNumber};
}

=head2 resultSetId ()

Get the SRU resultSetId that has been parsed.

=cut

sub resultSetId {
    my $self = shift;
    return $self->{resultSetId};
}

=head1 PRIVATE HANDLERS

This methods are private SAX handlers to parse the XML.

=head2 start_element

SAX handler for XML start tag. On PICA+ records this calls 
the start handler of L<PICA::XMLParser>, outside of records
it parses the SRU response.

=cut

sub start_element {
    my ($self, $el) = @_;

    if ($self->{in_record}) {

        # TODO: nasty hack because sru.gbv.de is broken:
        my ($tag) = grep { $_->{LocalName} eq 'tag' } values %{ $el->{Attributes} };
        if (defined $tag and $tag->{Value} eq '') {
            $self->{skip_field} = 1;
        } else {
            $self->{xmlparser}->start_element($el);
        }

    } else {
        $self->{char_data} = "";
        if ( _sru_element($el,"recordData") ) {
            #print "$name\n";
            $self->{in_record} = 1;
        }
    }
}

sub _sru_element {
    my ($el, $name) = @_; 
    return $el->{LocalName} eq $name and $el->{NamespaceURI} eq 'http://www.loc.gov/zing/srw/';
}

=head2 end_element

SAX handler for XML end tag. On PICA+ records this calls 
the end handler of L<PICA::XMLParser>.

=cut

sub end_element {
    my ($self, $el) = @_;

    if ($self->{in_record}) {
        if ( _sru_element($el,"recordData") ) {
            $self->{currentNumber}++;
            $self->{in_record} = 0;
        } else {
            if ( $self->{skip_field} ) { # nasty hack because sru.gbv.de is broken
                $self->{skip_field} = 0 if $el->{LocalName} eq 'datafield';
            } else {
                $self->{xmlparser}->end_element($el);
            }
        }
    } else {
        if ( _sru_element($el,"numberOfRecords") ) {
            $self->{numberOfRecords} = $self->{char_data};
        } elsif ( _sru_element($el,"resultSetId") ) {
            $self->{resultSetId} = $self->{char_data};
        }
    }
}

=head2 characters

SAX handler for XML character data. On PICA+ records this calls 
the character data handler of L<PICA::XMLParser>.

=cut

sub characters {
    my ($self, $data) = @_;

    if ($self->{in_record}) {
        $self->{xmlparser}->characters($data);
    } else {
        ($data) = values %$data;
        $self->{char_data} .= $data;
    }
}

1;

=encoding utf-8
