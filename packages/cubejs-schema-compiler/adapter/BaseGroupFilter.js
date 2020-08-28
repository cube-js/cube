
class BaseGroupFilter  { 
    constructor(query, filter) { 
        this.values = filter.values
        this.operator = filter.operator
        this.measure = filter.measure
        this.dimension = filter.dimension 
    }
      
    filterToWhere() { 
      const q = this.values.map(f => `(${f.filterToWhere()})`).join(` ${this.operator} `)
      console.log("BaseGroupFilter:filterToWhere", q)
      return q
    } 
  
    // @todo Use recursion
    path() {
    //   return this.values.map(f => f.path())
      return this.values[0].path()
    }
  
    // @todo Use recursion
    cube() {
    //   return this.values.map(f => f.cube()) 
      return this.values[0].cube()
    }
  
    // @todo Use recursion
    definition() {
        return this.values[0].definition();
    }
     
  }
  
module.exports = BaseGroupFilter;
