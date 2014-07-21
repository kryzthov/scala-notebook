package com.bwater.notebook

import com.bwater.notebook.util.Logging

import unfiltered.request.& // sadly...
import unfiltered.request.GET
import unfiltered.request.POST
import unfiltered.request.Params
import unfiltered.request.Path
import unfiltered.response.Pass

class ReqLogger extends Logging {
  val intent: unfiltered.netty.cycle.Plan.Intent = {
    case req @ GET(Path(p)) => {
      LOG.info("GET {}", req.uri)
      Pass
    }
    case req @ POST(Path(p) & Params(params)) => {
      LOG.info("POST {}: {}", p, params)
      Pass
    }
  }
}
