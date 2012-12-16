package Data::Workflow::GraphAlgorithms;
use Carp;
use Graph;
use strict;

# mixin that provides graph algorithms to Workflow
# see also Data::Workflow::BFSort

sub subgraph {
  my($self,$graph,@nodes)=@_;
  my $subgraph=new Graph(directed=>$graph->is_directed);
  $subgraph->add_vertices(@nodes);
  # add all edges amongst the nodes
  for my $node (@nodes) {
    my @successors=grep {$subgraph->has_vertex($_)} $graph->successors($node);
    map {$subgraph->add_edge($node,$_)} @successors;
  }
  $subgraph;
}
# Methods below only work for acyclic graphs
sub ancestors {
  my($self,$graph,@nodes)=@_;
  my @next=_flatten(@nodes);
  my %ancestors;
  @ancestors{@next}=@next;
  while (@next) {
    my @predecessors=map {$graph->predecessors($_)} @next;
    @next=grep {!$ancestors{$_}} @predecessors;
    @ancestors{@predecessors}=@predecessors;
  }
  wantarray? values %ancestors: [values %ancestors];
}
sub descendants {
  my($self,$graph,@nodes)=@_;
  my @next=_flatten(@nodes);
  my %descendants;
  @descendants{@next}=@next;
  while (@next) {
    my @successors=map {$graph->successors($_)} @next;
    @next=grep {!$descendants{$_}} @successors;
    @descendants{@successors}=@successors;
  }
  wantarray? values %descendants: [values %descendants];
}
sub ancestor_subgraph {
  my($self,$graph,@nodes)=@_;
  my @ancestors=$self->ancestors($graph,@nodes);
  $self->subgraph($graph,@ancestors);
}
sub descendant_subgraph {
  my($self,$graph,@nodes)=@_;
  my @descendants=$self->descendants($graph,@nodes);
  $self->subgraph($graph,@descendants);
}
# splice out a node, replacing it with a list of nodes
sub splice_node {
  my($self,$graph,$old,@new)=@_;
  my @predecessors=$graph->predecessors($old);
  my @successors=$graph->successors($old);
  for my $pred (@predecessors) {
    $graph->add_edges(map {[$pred,$_]} @new);
  }
  for my $succ (@successors) {
    $graph->add_edges(map {[$_,$succ]} @new);
  }
  $graph->delete_vertex($old);
}

sub _flatten {map {'ARRAY' eq ref $_? @$_: $_} @_;}
1;
