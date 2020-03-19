package ImportProvision;
require Exporter;
@ISA = qw(Exporter);
@EXPORT = qw(Iterator importiertDaten);

use strict;

use File::Path qw/remove_tree/;
use File::Temp qw/tempfile tempdir/;
use Archive::Zip;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use XML::Simple;
use Time::Piece;
use DBI;
use Scalar::MoreUtils qw(empty);
use Data::Dumper;
use Log::Log4perl;
use POSIX;
use warnings;

Log::Log4perl::init('log4perl.conf');
my $logger = Log::Log4perl->get_logger();


sub extrahiertDaten {

    my $xml_obj;
    my $xml_in;

    my $xlsx_datei = shift;

    my $extrakt = Archive::Zip->new($xlsx_datei);

    $extrakt->extractMemberWithoutPaths('xl/worksheets/sheet2.xml');

    open DATA , "<", "sheet2.xml"
	or die "Kann sheet2.xml nicht öffnen: $!\n";
    
    $xml_obj = XML::Simple->new();

    if ( -e "sheet2.xml" ) {
	$xml_in = $xml_obj->XMLin(\*DATA) or die "sheet2.xml nicht gefunden: $!\n";
    }

    unlink "sheet2.xml";

    return sub {
	
	my ($wkn, $provision, $gueltig_bis);
	
	while (my ($k, $v) = each @{$xml_in->{sheetData}->{row}}) {
	    if ( $k > 0 ) {
		
		$wkn = $v->{c}->[-1]->{v} ? $v->{c}->[-1]->{v} : return;
		$wkn = substr $wkn, 2, 6;
		
		$gueltig_bis = $v->{c}->[-3]->{v} ? $v->{c}->[-3]->{v} : return;
		$gueltig_bis = localtime(($gueltig_bis-25569)*86400)->strftime('%F'); # see stackoverflow.com for explanation: Excel Timestamp to Unix Timestamp
		
		$provision = $v->{c}->[-4]->{v} ? $v->{c}->[-4]->{v} : return;
		$provision = sprintf "%.2g", $provision * 100;
		$provision = sprintf "%.2f", $provision;
	    }
	    return [$wkn, $provision, $gueltig_bis];
	}
    };
    return;
}


sub importiertDaten {

    my $err;

    $logger->info('importiertDaten Beginn');

    if ( -e "importiertDaten.db" ) {
	unlink "importiertDaten.db";
    }

    my $dbh = DBI->connect("DBI:SQLite:dbname=importiertDaten.db", "", "", { PrintError=>1, RaiseError=>0 })
	or die "Kann nicht mit Datenbank verbinden: $DBI::errstr\n";

    $dbh->do("CREATE TABLE importiertDaten(id integer primary key, wkn text, provision text, gueltig_bis text, lokalzeit date default (datetime('now', 'localtime')))")
	or die "Kann SQL-Anweisung nicht ausführen: $dbh->errstr()\n";

    my $sql = "INSERT INTO importiertDaten (wkn, provision, gueltig_bis) values(?, ?, ?)";
    my $sth_1 = $dbh->prepare($sql)
	or die "Kann SQL-Anweisung nicht vorbereiten: $dbh->errstr()\n";

    $sql = "SELECT COUNT(id), wkn, provision, gueltig_bis FROM importiertDaten group by provision";
    my $sth_2 = $dbh->prepare($sql)
	or die "Kann SQL-Anweisung nicht vorbereiten: $dbh->errstr()\n";

    my $file = shift;

    my $xtrDaten = extrahiertDaten($file);

    my $count = 1;
    
    while ( my $values = $xtrDaten->() ) {
	
	my $wkn;
	my $provision;
	my $gueltig_bis;
	
	if ( !empty($values) ) {
	    $wkn         = $values->[0];
	    $provision   = $values->[1];
	    $gueltig_bis = $values->[2];
	}

	if ( $wkn && $provision && $gueltig_bis ) {
	    $sth_1->execute($wkn, $provision, $gueltig_bis)
		or die "Kann SQL-Anweisung nicht ausführen: $sth_1->errstr()\n";
	    if ( $count == 1 ) {
		my $t = sprintf strftime "%H:%M:%S", localtime;
		print STDERR sprintf "Erster  Datensatz %10d: %6s|%4s|%6s|übertragen um %s\n", $count, $wkn, $provision, $gueltig_bis, $t;
	    }
	    my $t = sprintf strftime "%H:%M:%S", localtime;
	    print STDERR sprintf "\33[KLetzter Datensatz %10d: %6s|%4s|%6s|übertragen um %s\r", $count++, $wkn, $provision, $gueltig_bis, $t;
	    $logger->info("$wkn|$provision|$gueltig_bis\n");
	}
    }

    print "\n";

    $sth_2->execute
	or die "Kann SQL-Anweisung nicht ausführen: $dbh->errstr()\n";

    $sql = "SELECT AVG(provision) FROM importiertDaten";
    my $avg = $dbh->selectrow_array($sql)
	or die "Kann SQL-Anweisung nicht vorbereiten: $dbh->errstr()\n";

    print "\n";

    while ( my @row = $sth_2->fetchrow_array() ) {
	printf "%.2f Prozent Provision: %d Datensätze\n", $row[2], $row[0];
	printf "BEISPIEL:  WKN %6s  PROVISION %.2f Prozent  GÜLTIKEIT BIS %10s\n\n", $row[1], $row[2], $row[3]; 
    }

    printf "Durchschnittliche Provision: %.2f Prozent", $avg;

    print "\n";

    $dbh->disconnect();

    $logger->info('importiertDaten Ende');
}

1;
