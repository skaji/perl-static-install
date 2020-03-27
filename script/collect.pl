#!/usr/bin/env perl
use 5.30.2;

use HTTP::Tiny;
use JSON::XS ();
use Time::Moment;
use experimental 'signatures';

package Release {
    sub new ($class, %argv) {
        my $date = $argv{date};
        $date .= "Z" if $date !~ /Z$/;
        bless { %argv, date => Time::Moment->from_string($date) }, $class;
    }
    sub name ($self) { $self->{name} }
    sub author ($self) { $self->{author} }
    sub distribution ($self) { $self->{distribution} }
    sub date ($self) { $self->{date} }
    sub id ($self) { $self->{id} }
    sub x_static_install ($self) { $self->{x_static_install} }
    sub first ($self) { $self->{first} }
}

package App {
    sub new ($class, %argv) {
        bless {
            %argv,
            url => 'https://fastapi.metacpan.org/v1/release/_search',
            http => HTTP::Tiny->new(timeout => 10, agent => 'https://github.com/skaji/perl-static-install'),
            json => JSON::XS->new->utf8->canonical,
        }, $class;
    }
    sub url ($self) { $self->{url} }
    sub http ($self) { $self->{http} }
    sub json ($self) { $self->{json} }

    sub request ($self, $body) {
        my $res = $self->http->post($self->url, {
            headers => { 'Content-Type' => 'application/json' },
            content => $self->json->encode($body),
        });
        if (!$res->{success}) {
            return (undef, "$res->{status} $res->{reason}");
        }
        my $c = $self->json->decode($res->{content});
        return ($c, undef);
    }

    sub recent ($self, $size, $from) {
        my $date = $from ? $from->date : Time::Moment->now_utc;
        my %query;
        $query{bool}{must}{range}{date}{lte} = $date->strftime("%Y-%m-%dT%H:%M:%S");
        $query{bool}{must_not}{term}{_id} = $from->id if $from && $from->id;

        my @field = qw(name author distribution date first);
        my $body = {
            size => $size // 20,
            query => \%query,
            _source => 'metadata.x_static_install',
            fields => \@field,
            sort => [ { date => { order => 'desc' } } ],
        };
        my ($res, $err) = $self->request($body);
        if ($err) {
            return (undef, $err);
        }
        my @release;
        for my $item ($res->{hits}{hits}->@*) {
            my $x_static_install = 'NA';
            if (exists $item->{_source}{metadata}{x_static_install}) {
                $x_static_install = $item->{_source}{metadata}{x_static_install};
            }
            my $release = Release->new(
                id => $item->{_id},
                x_static_install => $x_static_install,
                (map { ($_, $item->{fields}{$_}) } @field),
                first => $item->{fields}{first} ? 'true' : 'false',
            );
            push @release, $release;
        }
        return (\@release, undef);
    }
}

package File {
    sub new ($class, %argv) {
        bless { %argv }, $class;
    }
    sub path ($self) { $self->{path} }

    my @column = qw(date name author distribution x_static_install id first);

    sub _open ($self, $month) {
        my $path = sprintf "%s-%s.tsv", $self->path, $month;
        open my $fh, ">>:unix", $path or die "$path: $!";
        if (($fh->stat)[7] == 0) {
            $fh->say(join "\t", @column);
        }
        $fh;
    }

    sub write ($self, @release) {
        my %fh;
        for my $r (@release) {
            my $month = $r->date->strftime("%Y%m");
            my $fh = $fh{$month} ||= $self->_open($month);
            $fh->say(join "\t", map { $r->$_() } @column);
        }
    }

    sub last_release ($self) {
        my @file = sort glob $self->path . "-*.tsv";
        return if !@file;
        open my $fh, "<", $file[0] or die "$file[0]: $!";
        my $last;
        while (<$fh>) { $last = $_ }
        chomp $last;
        my @c = split /\t/, $last;
        Release->new( map { ($column[$_] => $c[$_]) } 0..$#c );
    }
}

my $app = App->new;
my $file = File->new(path => 'release/release');

my $previous = $file->last_release;
for my $i (1..10) {
    my ($rs, $err);
    for my $retry (1..3) {
        ($rs, $err) = $app->recent(500, $previous);
        last if !$err;
        if ($retry < 3) {
            warn "-> $err, sleep 60, retry...\n";
            sleep 60;
        } else {
            die "Giveup $err\n";
        }
    }
    $file->write($rs->@*);
    $previous = $rs->[-1];
    warn "finised ", $previous->date->to_string, "\n";
    sleep 5;
}
