import React, { Component } from 'react';
import { connect } from 'react-redux'
import { addToCart } from './actions/cartActions'

 class Home extends Component{
    
    handleClick = (id)=>{
        this.props.addToCart(id); 
        alert("Item added in to cart");
    }

    render(){
        let itemList = this.props.items.map(item=>{
            return(
                <div className="card" key={item.id}>
                        <div className="card-image">
                            <a><img src={item.img} alt={item.title}/></a>
                           
                        </div>

                        <div className="card-content">
                        <span className="card-title">{item.title}</span>
                            
                            <p>{item.desc}</p>
                            <p><b>Price: ${item.price}</b> <span to="/" className="btn-floating halfway-fab waves-effect waves-light blue" onClick={()=>{this.handleClick(item.id)}}>&nbsp;&nbsp;&nbsp;Add to Cart</span></p>
                        </div>
                 </div>

            )
        })

        return(
            <div className="container">
                <h3 className="center">Mobile Phones</h3>
                <div className="box">
                    {itemList}
                </div>
            </div>
        )
    }
}
const mapStateToProps = (state)=>{
    return {
      items: state.items
    }
  }
const mapDispatchToProps= (dispatch)=>{
    
    return{
        addToCart: (id)=>{dispatch(addToCart(id))}
    }
}

export default connect(mapStateToProps,mapDispatchToProps)(Home)