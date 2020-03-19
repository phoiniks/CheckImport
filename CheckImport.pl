#!/usr/bin/perl -w
use strict;
use warnings;
use diagnostics;
use Carp 'carp';
use lib '.';
use Import::ImportProvision;
use Log::Log4perl;

Log::Log4perl::init('log4perl.conf');
my $logger = Log::Log4perl->get_logger();

$logger->info("BEGINN");

ImportProvision::importiertDaten($ARGV[0]);

$logger->info("PROGRAMM ERFOLGREICH BEENDET");
