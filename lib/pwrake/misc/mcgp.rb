require "rbmetis"
#require "pwrake/grviz"

module Pwrake

  module MCGP

    def graph_partition(host_map, target=nil)
      t1 = Time.now
      wgts = host_map.group_weight_sum
      if wgts.size > 1
        list = wgts.size.times.to_a
        g = GraphTracerGroup.new([list],[wgts])
        trace(g,target)
        g.part_graph
        #g.write_dot('dag1.dot')
        #return
      end

      #$debug=true

      list = host_map.group_hosts
      wgts = host_map.group_core_weight
      g = GraphTracerNode.new(list,wgts)
      trace(g,target)
      g.part_graph
      t2 = Time.now
      Pwrake::Log.info "Time for TOTAL Graph Partitioning: #{t2-t1} sec"
      #g.write_dot('dag2.dot')
      #exit
    end
    module_function :graph_partition

    def trace(g,target)
      if target
        g.trace(target)
      else
        Rake.application.top_level_tasks.each do |t|
          g.trace(t)
        end
      end
    end
    module_function :trace

  end


  class GraphTracer

    def initialize(loc_list, weight_list)
      if loc_list.size != weight_list.size
        raise ArgumentError, "array size of args mismatch"
      end
      @loc_list = loc_list
      @weight_list = weight_list
      @n_part = @loc_list.size
      @traced = {}
      @vertex_depth = {}
      # @grviz = Grviz.new
      @group_list = @n_part.times.map do |i|
        GraphGroup.new(@loc_list[i],@weight_list[i],@vertex_depth,@grviz)
      end
    end

    def trace(name="default", target=nil)

      task = Rake.application[name]
      tw = task.wrapper
      group_id = tw.group_id || 0
      group = @group_list[group_id]
      #loc_list = @loc_list[group_id]
      depth = 0

      if task.class == Rake::FileTask
        tgid = (target) ? (Rake.application[target].wrapper.group_id||0) : nil

        if File.file?(name)
          if tgid == group_id
            locs = get_location(tw)
            #if locs.empty?
            #  Pwrake.application.postprocess(task)
            #  locs = get_location(tw)
            #end
            #tw.get_file_stat
            fsz = tw.file_size
            if fsz > 100000
              #puts "g=#{group_id}, task=#{name}, target=#{target}, fsz=#{fsz}, locs="+locs.join("|")
              group.push_loc_edge( locs, name, target, fsz/10000 )
            end
          else
            #puts "g=#{group_id}, task=#{name}, tgid=#{tgid}, target=#{target}"
          end
          return depth
        end

        group.push_vertex( name )
        if tgid == group_id
          #puts "g=#{group_id}, task=#{name}, target=#{target}"
          group.push_edge( name, target, nil )
        end
        target = name
      end

      if !@traced[name]
        @traced[name] = true

        task.prerequisites.each do |prereq|
          d = trace( prereq, target )
          depth = d if d and d > depth
        end

        if task.class == Rake::FileTask
          depth += 1
        end

        @vertex_depth[name] = depth
      end

      return @vertex_depth[name]
    end

    def write_dot(file)
      @grviz.write(file)
    end
  end


  class GraphTracerGroup < GraphTracer

    def get_location(tw)
      tw.group
    end

    def part_graph
      @group_list.each do |g|
        g.part_graph
        g.set_group
      end
    end
  end


  class GraphTracerNode < GraphTracer

    def get_location(tw)
      tw.location
    end

    def part_graph
      @group_list.each do |g|
        g.part_graph
        g.set_node
      end
    end
  end


  class GraphGroup

    def summation(a)
      s = 0
      a.each{|x| s+=x}
      s
    end

    def normalize(a)
      s = summation(a).to_f
      (s==0) ? a : a.map{|x| x/s}
    end

    def average(a)
      s = summation(a).to_f
      (a.empty?) ? nil : s/a.size
    end

    def initialize(loc_list, weight_list, vertex_depth, grviz)
      if loc_list.size != weight_list.size
        raise ArgumentError, "array size mismatch"
      end
      @n_part = loc_list.size
      a = [loc_list, weight_list].transpose
      a.sort_by!{|x| x[1]}
      b = a.transpose
      @loc_list = b[0]
      @tpwgts = normalize(b[1])

      b = @tpwgts[0]
      a = @tpwgts[-1]-b
      if a/b > 1e-3
        @host_wgts = @tpwgts.map{|x| (((x-b)/a*0.45+1)*1000).to_i}
      else
        @host_wgts = @tpwgts.map{|x| 1000}
      end

      @vertex_name2id = {}
      @vertex_id2name = []
      @edges = []
      @file_sizes = []
      @loc_files = {}
      @count = 0

      @vertex_depth = vertex_depth
      @grviz = grviz

      @loc_list.each do |loc|
        push_vertex(loc)
        @vertex_depth[loc] = 0
      end
    end

    def push_loc_edge(locs, name, target, fsz)
      locs.each do |loc|
        if @loc_list.include?(loc)
          push_edge(loc, target, fsz)
          @loc_files[loc] ||= []
          @loc_files[loc] << name
        end
      end
      @file_sizes << fsz
      #p [object_id,target,fsz]
    end

    def push_vertex(name)
      if !@vertex_name2id.has_key?(name)
        @vertex_name2id[name] = @count
        @vertex_id2name[@count] = name
        @grviz.push_vertex(name) if @grviz
        @count += 1
      end
    end

    def push_edge(name, target, weight)
      if target and (weight.nil? or weight>0)
        push_vertex(name)
        push_vertex(target)
        v1 = @vertex_name2id[name]
        v2 = @vertex_name2id[target]
        (@edges[v1] ||= []).push [v2, weight]
        (@edges[v2] ||= []).push [v1, weight]
        @grviz.push_edge(name, target) if @grviz
      end
    end

    def part_graph
      @xadj = [0]
      @adjcny = []
      @adjwgt = []
      @vwgt = []

      depth_hist = []
      @vertex_id2name.each do |name|
        depth = @vertex_depth[name]
        # puts "name=#{name}, depth=#{depth}"
        depth_hist[depth] = (depth_hist[depth] || 0) + 1
      end

      map_depth = []
      ubv = []
      c = 0
      depth_hist.each do |x|
        if x and x>=@n_part
          map_depth << c
          c += 1
          ubv << 1 + 0.5*@n_part/x
          #ubv << ((x >= @n_part) ? 1.05 : 1.5)
        else
          map_depth << nil
        end
      end
      ubv[0] = 1.0005

      Pwrake::Log.info "loc_list=#{@loc_list}"
      Pwrake::Log.info "partition_weights=#{@tpwgts}"
      Pwrake::Log.info "ncon=#{c}"
      Pwrake::Log.info "depth_hist=#{depth_hist.inspect}"
      Pwrake::Log.info "ubvec=#{ubv.inspect}"

      if @file_sizes.empty?
        @edge_weight = 1
      else
        @edge_weight = average(@file_sizes).to_i*3
      end
      Pwrake::Log.info "default_edge_weight=#{@edge_weight}"

      @vertex_id2name.size.times do |i|
        if edg = @edges[i]
          edg.sort_by!{|x| x[0]}
          @adjcny.concat(edg.map{|x| x[0]})
          @adjwgt.concat(edg.map{|x| x[1] || @edge_weight})
          # @adjwgt.concat(edg.map{|x| x[1] ? 0 : @edge_weight})
        end
        @xadj.push(@adjcny.size)
      end

      @vertex_id2name.each_with_index do |name,i|
        w = Array.new(c,0)
        if i < @n_part
          w[0] = @host_wgts[i]
          # puts "name=#{name}, w=#{w.inspect}"
        else
          depth = @vertex_depth[name]
          if depth and (j = map_depth[depth])
            w[j] = 1
          end
        end
        @vwgt.concat(w)
      end

      #  puts "@vertex_id2name[#{i}]=#{@vertex_id2name[i]}, depth=#{depth}, edges="+@edges[i].map{|x| @vertex_id2name[x[0]]}.join("|")

      t1 = Time.now
      if false
        puts "@vertex_id2name.size=#{@vertex_id2name.size}"
        if $debug2
          @vertex_id2name.each_with_index{|x,i|
            puts "#{i} #{x} #{@edges[i].inspect}"
          }
        end
        puts "@edges.size=#{@edges.size}"
        puts "ncon=#{c}"
        puts "@n_part=#{@n_part}"
        puts "@xadj.size=#{@xadj.size}"
        puts "@adjcny.size/2=#{@adjcny.size/2}"
        puts "@adjwgt.size/2=#{@adjwgt.size/2}"
        puts "@vwgt.size=#{@vwgt.size}"
        puts "@vwgt.size/ncon=#{@vwgt.size/c}"
        puts "depth_hist=#{depth_hist.inspect}"
        puts "ubv=#{ubv.inspect}"
        if $debug
          puts "@xadj=#{@xadj.inspect}"
          puts "@adjcny=#{@adjcny.inspect}"
          puts "@adjwgt=#{@adjwgt.inspect}"
          puts "@vwgt=#{@vwgt.inspect}"
        end
        #exit
      end
      if defined? RbMetis
        hw = normalize(@host_wgts)
        tpw = []
        s = "tpwgts=[\n"
        @tpwgts.each_with_index do |x,i|
          a = [hw[i]]+[x]*(c-1)
          tpw.concat(a)
          s += " ["+a.map{|x|"%.5f"%x}.join(", ")+"]\n"
        end
        s += "]"
        Log.info s
        options = RbMetis.default_options
        options[RbMetis::OPTION_NCUTS] = 30
        options[RbMetis::OPTION_NSEPS] = 30
        options[RbMetis::OPTION_NITER] = 10
        @part = RbMetis.part_graph_recursive(
                  @xadj, @adjcny, @n_part,
                  ncon:c, vwgt:@vwgt, adjwgt:@adjwgt,
                  tpwgts:tpw, ubvec:ubv, options:options)
      else
        puts "tpw=#{tpw.inspect}"
        @part = Metis.mc_part_graph_recursive2(
                  c,@xadj,@adjcny,@vwgt,nil,@tpwgts)
      end
      t2 = Time.now
      Pwrake::Log.info "Time for Graph Partitioning: #{t2-t1} sec"
      count_partition
      if $debug
        puts "Time for Graph Partitioning: #{t2-t1} sec"
        p @part
      end
    end

    def count_partition
      locs = Array.new(@n_part,nil)
      @n_part.times do |i|
        i_part = @part[i]
        locs[i_part] ||= []
        locs[i_part] << @vertex_id2name[i]
      end
      # p locs
      sum = []
      @vertex_id2name.each_with_index do |name,idx|
        y = @vertex_depth[name]
        x = @part[idx]
        sum[y] ||= Array.new(@n_part,0)
        sum[y][x] += 1
      end
      s = "partition count: \n"
      s += sum.each_with_index.map do |row,idx|
        " d=#{idx} "+row.inspect
      end.join("\n")
      #puts s
      Log.info s
      Log.info "@part[0:#{@n_part-1}]=#{@part[0...@n_part].inspect}"
      sum[0].each{|i| raise RuntimeError,"Unequal partitioning" if i!=1}
    end


    def count_partition2(part)
      sum = Array.new(0,0)
      part.each do |x|
        sum[x] ||= 0
        sum[x] += 1
      end
      s = sum.each_with_index.map do |x,i|
        "#{i}:#{x}"
      end
      puts "n_nodes=[ "+s.join(", ")+" ]"
      puts "@part[0:#{@n_part-1}]=#{part[0...@n_part].inspect}"
    end

    def make_loc_list
      rest = []
      loc_list = []
      @n_part.times do |i|
        i_part = @part[i]
        loc = @loc_list[i]
        if loc_list[i_part]
          rest << loc
        else
          loc_list[i_part] = loc
        end
      end
      @n_part.times do |i|
        unless loc_list[i]
          loc_list[i] = rest.pop
        end
      end
      loc_list
    end


    def set_group
      loc_list = make_loc_list()
      @vertex_id2name.each_with_index do |name,idx|
        if idx >= @n_part
          i_part = @part[idx]
          tw = Rake.application[name].wrapper
          tw.group_id = loc_list[i_part]
          #puts "task=#{task.inspect}, i_part=#{i_part}, host=#{host}"
        end
      end
      @loc_files.each do |gid,files|
        files.each do |f|
          tw = Rake.application[f].wrapper
          tw.group_id = gid
          # puts "gid=#{gid}, task=#{f}"
        end
      end
    end


    def set_node
      loc_list = make_loc_list()
      @vertex_id2name.each_with_index do |name,idx|
        if idx >= @n_part
          i_part = @part[idx]
          tw = Rake.application[name].wrapper
          host = loc_list[i_part]
          tw.suggest_location = [host]
          #puts "task=#{task.inspect}, i_part=#{i_part}, host=#{host}"
        end
      end
    end

  end # class MetisGraph

end
