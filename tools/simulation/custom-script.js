 // data generation
    var values = [78.9, 78.9, 78.9, 75.1, 68.0, 67.0, 65.6, 65.1, 61.9, 50.4, 59.6, 59.6, 60.5, 80.1, 75.3, 78.8, 79.0, 110.0, 25.0, 20.3, 53.7, 59.6, 61.6, 63.6, 66.1, 250.1, 66.1, 60.2, 80.01, 2.0, 2.0];

    function previous_week(today){
        var nextweek = new Date(today.getFullYear(), today.getMonth(), today.getDate()-7);
        return nextweek;
    }
    var today = new Date();

    var dates = [];
    for(var i = values.length; i--;){
        dates[i] = today;
        today = previous_week(today);
    }

    const source = [];
    for(var i =0; i < values.length; i++){

        source.push({time_bin: dates[i].toISOString().slice(0,10), value:values[i]});
    }

    //
    var threshold = 10;

    function removeContainer(container_id){
        const barContainer = d3.select('#' + container_id);
        const svgElem = barContainer.select('svg');

        svgElem.selectAll("*").remove();
    }

    function drawBar(data, container_id, traffic){
        const barContainer = d3.select('#' + container_id);
        const svgElem = barContainer.select('svg');

        const margin = 100;
        const width = 1000 - 2 * margin;
        const height = 600 - 2 * margin;

        const chartElem = svgElem.append('g')
          .attr('transform', `translate(${margin}, ${margin})`);

        const xScaleBar = d3.scaleBand()
          .range([0, width])
          .domain(data.map((s) => s.time_bin))
          .padding(0.4)

        const yScaleBar = d3.scaleLinear()
          .range([height, 0])
          .domain([0, d3.max(data.map((s) => s.value))]);

        const makeYLinesBar = () => d3.axisLeft()
          .scale(yScaleBar)

        chartElem.append('g')
          .attr('transform', `translate(0, ${height})`)
          .call(d3.axisBottom(xScaleBar))
          .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(-45)");

        chartElem.append('g')
          .call(d3.axisLeft(yScaleBar));

        chartElem.append('g')
          .attr('class', 'grid')
          .call(makeYLinesBar()
            .tickSize(-width, 0, 0)
            .tickFormat('')
          )

        const barGroupsBar = chartElem.selectAll()
          .data(data)
          .enter()
          .append('g')

        barGroupsBar
          .append('rect')
          .attr('class', (g) => ('bar time-'+ g.time_bin))
          .attr('x', (g) => xScaleBar(g.time_bin))
          .attr('y', (g) => yScaleBar(g.value))
          .attr('height', (g) => height - yScaleBar(g.value))
          .attr('width', xScaleBar.bandwidth())
          .on('mouseenter', function (actual, i) {
                d3.select(this)
                  .transition()
                  .duration(300)
                  .style('fill', 'red')
                  .attr('opacity', 0.6)

                d3.selectAll('.time-' + actual.time_bin)
                  .transition()
                  .duration(300)
                  .style('fill', 'red')
                  .attr('opacity', 0.6)

                const y = yScaleBar(actual.value)

                /*
                barGroupsBar.append('text')
                  .attr('class', 'divergence')
                  .attr('x', (a) => xScale(a.time_bin) + xScale.bandwidth() / 2)
                  .attr('y', (a) => yScale(a.value) + 30)
                  .attr('fill', 'white')
                  .attr('text-anchor', 'middle')
                  .text((a, idx) => {
                    const divergence = (a.value - actual.value).toFixed(1)

                    let text = ''
                    if (divergence > 0) text += '+'
                    text += `${divergence}%`

                    return idx !== i ? text : a.value +'%';
                  })*/

              })
              .on('mouseleave', function (actual) {
                d3.select(this)
                  .transition()
                  .duration(300)
                  .style('fill', '#80cbc4')
                  .attr('opacity', 1)
                  .attr('x', (a) => xScaleBar(a.time_bin))
                  .attr('width', xScaleBar.bandwidth())

                d3.selectAll('.time-' + actual.time_bin)
                  .transition()
                  .duration(300)
                  .style('fill', '#80cbc4')
                  .attr('opacity', 1)
                  .attr('x', (a) => xScaleBar(a.time_bin))
                  .attr('width', xScaleBar.bandwidth())

                chartElem.selectAll('#limit').remove()
                chartElem.selectAll('.divergence').remove()
              })

        barGroupsBar
          .append('text')
          .attr('class', 'value')
          .attr('x', (a) => xScaleBar(a.time_bin) + xScaleBar.bandwidth() / 2)
          .attr('y', (a) => yScaleBar(a.value) + 30)
          .attr('text-anchor', 'middle')
          .text((a) => `${a.value}`)

        svgElem
          .append('text')
          .attr('class', 'label')
          .attr('x', -(height / 2) - margin)
          .attr('y', margin / 2.4)
          .attr('transform', 'rotate(-90)')
          .attr('text-anchor', 'middle')
          .text('Value of interest')

        svgElem.append('text')
          .attr('class', 'label')
          .attr('x', width / 2 + margin)
          .attr('y', height + margin * 1.7)
          .attr('text-anchor', 'middle')
          .text('Time')

        svgElem.append('text')
          .attr('class', 'source')
          .attr('x', width - margin / 2)
          .attr('y', height + margin * 1.7)
          .attr('text-anchor', 'start')
          .text('Source: popmon reference dataset')

        /*
        svg.append('text')
          .attr('class', 'title')
          .attr('x', width / 2 + margin)
          .attr('y', 40)
          .attr('text-anchor', 'middle')
          .text('Reference distribution - Value of Interest over time')
        */

        if(traffic != null){
            drawTL([yScaleBar(traffic[0]), yScaleBar(traffic[1]), yScaleBar(traffic[2]), yScaleBar(traffic[3])], container_id);
        }
    }

    function updateData() {
        threshold += 1;
        var new_sample = source.filter((d,i) => i < threshold);
        new_tls = compute_tl(new_sample)

        removeContainer('container');
        drawBar(new_sample, 'container');

        new_zSample = compute_zscore(new_sample);
        removeContainer('z_score')
        drawZscore(new_sample, new_zSample);

        removeContainer('container2');
        drawBar(subsample, 'container2', new_tls);

        removeContainer('histogram');
        drawHistogram('histogram', new_zSample, new_sample);

    }

    /*
    function removeTL(container_id){
        d3.selectAll('#'+container_id+' line#limit').remove();
        d3.selectAll('#'+container_id+' line#limitr').remove();
    }
    */

    function drawTL(traffic_lights, container_id){
        const barContainer = d3.select('#' + container_id);
        const svgElem = barContainer.select('svg');
        const chartElem = svgElem.select('g');

        const margin = 100;
        const width = 1000 - 2 * margin;
        const height = 600 - 2 * margin;

        chartElem.append('line')
          .attr('id', 'limitr')
          .attr('x1', 0)
          .attr('y1', traffic_lights[0])
          .attr('x2', width)
          .attr('y2', traffic_lights[0]);

        chartElem.append('line')
          .attr('id', 'limit')
          .attr('x1', 0)
          .attr('y1', traffic_lights[1])
          .attr('x2', width)
          .attr('y2', traffic_lights[1]);

        chartElem.append('line')
          .attr('id', 'limit')
          .attr('x1', 0)
          .attr('y1', traffic_lights[2])
          .attr('x2', width)
          .attr('y2', traffic_lights[2]);

        chartElem.append('line')
          .attr('id', 'limitr')
          .attr('x1', 0)
          .attr('y1', traffic_lights[3])
          .attr('x2', width)
          .attr('y2', traffic_lights[3]);
    }


    var sample = source.filter((d,i) => i < threshold);
    var tls = compute_tl(sample)

    drawBar(sample, 'container', null);

    var subsample = source.filter((d,i) => i > 20);
    drawBar(subsample, 'container2', tls);

    function compute_zscore(msample){
        mean = d3.mean(msample.map((s) => s.value))
        std = d3.deviation(msample.map((s) => s.value))
        return msample.map((a) => ({
          'time_bin': a.time_bin,
          'value': (a.value - mean) / std
        }));
    }

    function compute_tl(msample){
        mean = d3.mean(msample.map((s) => s.value))
        std = d3.deviation(msample.map((s) => s.value))
        return [mean - 7*std, mean -4* std, mean + 4*std, mean +7*std]
    }

    zSample = compute_zscore(sample);


    const svgContainer = d3.select('#container');
    const svg = svgContainer.select('svg');

    const margin = 100;
    const width = 1000 - 2 * margin;
    const height = 600 - 2 * margin;

    function drawZscore(data, zdata){
        // Other diagram
        const zContainer = d3.select('#z_score');
        const zSvg = zContainer.select('svg');

        const margin = 80;
        const margin2 = 80;
        const width = 1000 - 2 * margin;
        const width2 = 1000 - 2 * margin2;
        const height = 600 - 2 * margin;
        const height2 = 600 - 2 * margin2;

        const xScale = d3.scaleBand()
          .range([0, width])
          .domain(data.map((s) => s.time_bin))
          .padding(0.4)

        const yScale = d3.scaleLinear()
          .range([height, 0])
          .domain([0, d3.max(data.map((s) => s.value))]);

        const makeYLines = () => d3.axisLeft()
          .scale(yScale)

        const chart2 = zSvg.append('g')
          .attr('transform', `translate(${margin2}, ${margin2})`);

        const barGroups2 = chart2.selectAll()
          .data(zdata)
          .enter()
          .append('g')

        chart2.append('g')
          .attr('transform', `translate(0, ${height})`)
          .call(d3.axisBottom(xScale))
          .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(-45)");

        const yScale2 = d3.scaleLinear()
          .range([height, 0])
          .domain([-7, 7]);

        chart2.append('g')
          .call(d3.axisLeft(yScale2));

        chart2.append('g')
          .attr('class', 'grid')
          .call(makeYLines()
            .tickSize(-width, 0, 0)
            .tickFormat('')
          )

        barGroups2
          .append('rect')
          .attr('class', (g) => ('bar time-'+ g.time_bin))
          .attr('x', (g) => xScale(g.time_bin))
          .attr('y', function(g){
                value = Math.max(g.value, 0);
                return yScale2(value)
           })
          .attr('height', (g) => Math.abs(yScale2(g.value) - (0.5 * height)))
          .attr('width', xScale.bandwidth())

        barGroups2
          .append('text')
          .attr('class', 'value')
          .attr('x', (a) => xScale(a.time_bin) + xScale.bandwidth() / 2)
          .attr('y', (a) => yScale2(a.value + 20))
          .attr('text-anchor', 'middle')
          .text((a) => `${a.value.toFixed(2)}`)

         zSvg
          .append('text')
          .attr('class', 'label')
          .attr('x', -(height / 2) - margin)
          .attr('y', margin / 2.4)
          .attr('transform', 'rotate(-90)')
          .attr('text-anchor', 'middle')
          .text('Pull')
       }

       drawZscore(sample, zSample);


      function drawHistogram(container_id, z_sample, msample){
          // Histogram

          const hContainer = d3.select('#'+ container_id);
          const hSvg = hContainer.select('svg');

          hSvg
              .append('text')
              .attr('class', 'label')
              .attr('x', -(height / 2) - margin)
              .attr('y', margin / 2.4)
              .attr('transform', 'rotate(-90)')
              .attr('text-anchor', 'middle')
              .text('Reference Pull')

          // X axis: scale and draw:
          var x = d3.scaleLinear()
              .domain([-7, 7])
              .range([0, width]);

          mean = d3.mean(msample.map((s) => s.value))
          std = d3.deviation(msample.map((s) => s.value))
          var xdub = d3.scaleLinear()
              .domain([mean - 7 * std , mean + 7 * std])
              .range([0, width]);

          const chart3 = hSvg.append("g")
              .attr('transform', `translate(${margin}, ${margin})`);

          chart3.append("g")
              .attr("transform", "translate(0," + height + ")")
              .call(d3.axisBottom(x));

        chart3.append("g")
              .attr("transform", "translate(0," + height * 1.1 + ")")
              .call(d3.axisBottom(xdub).ticks(13));

          // set the parameters for the histogram
          var histogram = d3.histogram()
              .value(function(d) { return d.value; })
              .domain(x.domain())
              .thresholds(x.ticks(20)); // then the numbers of bins

          // And apply this function to data to get the bins
          var bins = histogram(z_sample);

          // Y axis: scale and draw:
          var y = d3.scaleLinear()
              .range([height, 0]);
              y.domain([0, d3.max(bins, function(d) { return d.length; })]);

          hSvg.append("g")
              .call(d3.axisLeft(y));

          // append the bar rectangles to the svg element
          hSvg.selectAll("rect")
              .data(bins)
              .enter()
              .append("rect")
                .attr("x", margin)
                .attr("y", margin)
                .attr("transform", function(d) { return "translate(" + x(d.x0) + "," + y(d.length) + ")"; })
                .attr("width", function(d) { return x(d.x1) - x(d.x0) ; })
                .attr("height", function(d) { return y(0)  - y(d.length); })
                .style("fill", "#80cbc4")

          line = chart3.append('line')
              .attr('id', 'limit')
              .attr('x1', x(4))
              .attr('y1', 0)
              .attr('x2', x(4))
              .attr('y2', height)
          line = chart3.append('line')
              .attr('id', 'limit')
              .attr('x1', x(-4))
              .attr('y1', 0)
              .attr('x2', x(-4))
              .attr('y2', height)
            line = chart3.append('line')
              .attr('id', 'limitr')
              .attr('x1', x(7))
              .attr('y1', 0)
              .attr('x2', x(7))
              .attr('y2', height)
          line = chart3.append('line')
              .attr('id', 'limitr')
              .attr('x1', x(-7))
              .attr('y1', 0)
              .attr('x2', x(-7))
              .attr('y2', height)
    }

    drawHistogram('histogram', zSample, sample);