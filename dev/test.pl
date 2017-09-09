#!/usr/bin/env perl

use strict;
use warnings;
use v5.10;
use utf8;

use Cpanel::JSON::XS qw(encode_json);
use IO::Socket;

my $sock = IO::Socket::INET->new(
  Proto    => 'udp',
  PeerPort => 9001,
  PeerAddr => '172.17.0.1',
) or die "Could not create socket: $!\n";

for ( 1 .. 100 ) {
  my %val = (
    query   => 'INSERT INTO test (dt_part,dt,id) VALUES (?,?,?);',
    data    => [ "2017-09-09", "2017-09-09 12:26:03", $_ ],
    types   => [ "string", "string", "int" ],
    version => 1,
  );

  $sock->send( encode_json( \%val ) ) or die "Send error: $!\n";

  sleep 1;
}
