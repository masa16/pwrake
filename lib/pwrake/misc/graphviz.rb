module Pwrake

  class Graphviz

    def initialize
      @nodes = []
      @edges = []
      # @node_id = {}
      @filenode_id = {}
      @tasknode_id = {}
      @node_name = {}
      @count = 0
      @traced = {}
    end

    attr_reader :filenode_id, :tasknode_id, :node_name

    def trace( name = :default, target = nil )
      traced_cond = @traced[name]

      task = Rake.application[name]

      #if task.kind_of?(Rake::FileTask)
      if task.kind_of?(Rake::Task)
        push_filenode( name )
        if !task.actions.empty? and !traced_cond
          push_tasknode( name )
          push_taskedge( name )
        end
        push_fileedge( name, target )
        target = name
      end
      @traced[name] = true

      if !traced_cond
        task.prerequisites.each_with_index do |prereq,i|
          trace( prereq, target )
        end
      end
    end

    def trim( name )
      name = name.to_s
      name = File.basename(name)
      name.sub(/H\d+/,'').sub(/object\d+/,"")
    end

    def push_filenode( name )
      if @filenode_id[name].nil?
        tag = "T#{@count}"
        @count += 1
        @filenode_id[name] = tag
        @node_name[tag] = name
        @nodes.push "#{tag} [label=\"#{trim(name)}\", shape=box];"
      end
    end

    def push_tasknode( name )
      if @tasknode_id[name].nil?
        tag = "T#{@count}"
        @count += 1
        @tasknode_id[name] = tag
        @node_name[tag] = name
        label = Rake.application[name].comment
        @nodes.push "#{tag} [label=\"#{label}\", shape=ellipse];"
      end
    end

    def push_fileedge( name, target )
      if target
        if n2 = @tasknode_id[target]
          n1 = @filenode_id[name]
        elsif n1 = @tasknode_id[name]
          n2 = @filenode_id[target]
        else
          n1 = @filenode_id[name]
          n2 = @filenode_id[target]
        end
        @edges.push "#{n1} -> #{n2};"
      end
    end

    def push_taskedge( name )
      if n1 = @tasknode_id[name]
        n2 = @filenode_id[name]
        @edges.push "#{n1} -> #{n2};"
      end
    end

    def write(file)
      open(file, "w") do |w|
        #w.puts "digraph sample {\ngraph [size=\"12,100\",ranksep=1.5,nodesep=0.2];"
        w.puts "digraph sample {"
        w.puts "graph [size=\"70,70\", rankdir=LR];"
        @nodes.each do |x|
          w.puts x
        end
        @edges.each do |x|
          w.puts x
        end
        w.puts "}"
      end
    end
  end
end

task "graphviz", :file do |t,a|
  file = a[:file] || 'pwrake.dot'
  g = Pwrake::Graphviz.new
  g.trace
  g.write(file)
  $stderr.puts "Wrote task graph to `#{file}'"
end
